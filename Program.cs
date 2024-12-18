using Confluent.Kafka;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

public class Program
{
    private static long _generalErrors = 0;
    private static long _retryAttempts = 0;
    private static long _failedMessages = 0;
    private static long _discardedMessages = 0;
    private static long _processedMessages = 0;
    private static long _totalProcessingTimeMs = 0;
    private static SemaphoreSlim _semaphore; // Para control de tareas concurrentes
    private static int _timeToDiscardMessage = 3;

    public static async Task Main(string[] args)
    {
        // Validar la configuración de conexión a Kafka
        var topic = Environment.GetEnvironmentVariable("KAFKA_TOPICO_VEHICLE_LOCAL");
        var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_SERVER_LOCAL");
        var consumerGroup = Environment.GetEnvironmentVariable("KAFKA_CONSUMER_GROUP_LOCAL");

        if (string.IsNullOrWhiteSpace(bootstrapServers) ||
            string.IsNullOrWhiteSpace(consumerGroup) ||
            string.IsNullOrWhiteSpace(topic))
        {
            Console.WriteLine("Error: Existen variables de entorno para conectarse a Kafka no se encuentran configuradas correctamente.");
            Environment.Exit(1);
        }

        // Validar la configuración de reintentos
        int _maxRetries = 3;
        int _delayBetweenRetriesSeconds = 15;

        if (
        !int.TryParse(Environment.GetEnvironmentVariable("RECONNECT_MAX_TRIES"), out _maxRetries) &&
        !int.TryParse(Environment.GetEnvironmentVariable("RECONNECT_WAIT_SECONDS"), out _delayBetweenRetriesSeconds)
        )
        {
            Console.WriteLine("Error: Existen variables de entorno para la reconexion que no se encuentran configuradas correctamente.");
            Environment.Exit(1);
        }

        // Validar la configuración de tareas concurrentes
        int _maxConcurrentTasks = 10;

        if (!int.TryParse(Environment.GetEnvironmentVariable("MAX_PARALLEL_TASKS"), out _maxConcurrentTasks))
        {
            Console.WriteLine("Error: Existen variables de entorno para la concurrencia que no se encuentran configuradas correctamente.");
            Environment.Exit(1);
        }

        _semaphore = new SemaphoreSlim(_maxConcurrentTasks, _maxConcurrentTasks);

        // Validar la configuración de lectura de mensajes
        if (!int.TryParse(Environment.GetEnvironmentVariable("TIME_TO_DISCARD_MESSAGES"), out _timeToDiscardMessage))
        {
            Console.WriteLine("Error: Existen variables de entorno para la no lectura de mensajes viejos que no se encuentran configuradas correctamente.");
            Environment.Exit(1);
        }

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = consumerGroup,
            AutoOffsetReset = AutoOffsetReset.Latest, // Consumir desde el último offset confirmado
            EnableAutoCommit = true, // Activa el commit automático
            SessionTimeoutMs = 10000, // Tiempo límite para detectar desconexión
            MaxPollIntervalMs = 30000, // Límite de tiempo entre polls
            ReconnectBackoffMs = 100, // Retraso inicial de reconexion
            ReconnectBackoffMaxMs = 1000, // Retraso maximo de reconexion
        };

        var metricsTimer = new Timer(ShowMetrics, null, TimeSpan.Zero, TimeSpan.FromSeconds(30));

        // Intentar conectar y verificar la conexion inicial
        bool isConnected = await RetryConnectToCluster(bootstrapServers, _maxRetries, _delayBetweenRetriesSeconds);

        if (!isConnected)
        {
            Console.WriteLine("No se pudo conectar al cluster de Kafka despues de varios intentos. Finalizando el programa.");
            metricsTimer.Dispose();
            ShowMetrics(null);
            Environment.Exit(1);
        }

        using var consumer = new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler(async (_, e) =>
            {
                Console.WriteLine($"Error del cliente Kafka: {e.Reason}");
                Interlocked.Increment(ref _generalErrors);

                // Manejo explícito de errores:
                if (e.IsFatal || e.IsLocalError || e.IsBrokerError || e.IsError)
                {
                    Console.WriteLine("Error fatal detectado. Procediendo a reintentar...");
                    await HandleReconnection(bootstrapServers, _maxRetries, _delayBetweenRetriesSeconds);
                }
            })
            .Build();

        consumer.Subscribe(topic);

        Console.WriteLine("Conexion exitosa. Comenzando a consumir mensajes...");

        try
        {
            await ConsumeMessages(consumer, _maxRetries, _delayBetweenRetriesSeconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error inesperado: {ex.Message}");
        }

        metricsTimer.Dispose();
        ShowMetrics(null);
        Environment.Exit(1);
    }

    private static async Task HandleReconnection(string bootstrapServers, int maxRetries, int delayBetweenRetriesSeconds)
    {
        int retryCount = 0;
        bool isReconnected = false;

        // Intentar reconectar en caso de error fatal
        while (retryCount < maxRetries)
        {
            try
            {
                Console.WriteLine($"Reintentando la conexion al cluster de Kafka. Intento {retryCount + 1}/{maxRetries}...");
                var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                Console.WriteLine($"Conexion exitosa al cluster: {metadata.OriginatingBrokerName}");
                isReconnected = true;
                break; // Si la reconexion es exitosa, salimos del ciclo
            }
            catch (Exception ex)
            {
                retryCount++;
                Console.WriteLine($"Error al intentar reconectar: {ex.Message}. Reintentando en {delayBetweenRetriesSeconds} segundos...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("Se alcanzo el numero maximo de intentos de reconexion.");
                    break;
                }
                await Task.Delay(TimeSpan.FromSeconds(delayBetweenRetriesSeconds));
            }
        }

        if (!isReconnected)
        {
            Console.WriteLine("No se pudo reconectar al cluster de Kafka. Finalizando...");
            Environment.Exit(1);
        }
    }

    private static async Task ConsumeMessages(IConsumer<Ignore, string> consumer, int maxRetries, int delayBetweenRetriesSeconds)
    {
        int retryCount = 0;

        while (true)
        {
            try
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(5));

                if (cr != null)
                {
                    var stopwatch = Stopwatch.StartNew();

                    var messageTimestamp = cr.Message.Timestamp.UtcDateTime;

                    // Rechazar mensajes viejos (más de X minutos atrás)
                    if (messageTimestamp < DateTime.UtcNow.AddMinutes(_timeToDiscardMessage*(-1)))
                    {
                        Interlocked.Increment(ref _discardedMessages);
                        Console.WriteLine($"Descartado mensaje viejo {cr.Message.Value}");
                        continue;
                    }

                    stopwatch.Stop();

                    await ProcessMessageAsync(cr.Message.Value); // Usamos ProcessMessageAsync para manejar las tareas en paralelo
                    Interlocked.Add(ref _totalProcessingTimeMs, (long)stopwatch.Elapsed.TotalMilliseconds);
                    Interlocked.Increment(ref _processedMessages);
                    consumer.Commit(cr); // Confirmar offset
                }

                // Resetear reintentos despues de un mensaje exitoso.
                retryCount = 0;
            }
            catch (ConsumeException ex)
            {
                bool shouldExit = await HandleConsumeErrorAsync(ex, maxRetries, delayBetweenRetriesSeconds);
                if (shouldExit) break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inesperado: {ex.Message}");
                break; // Salir en caso de error no manejado.
            }
        }

        Interlocked.Increment(ref _generalErrors);
        throw new Exception("Fallo continuo en el consumo de mensajes. Finalizando flujo.");
    }

    private static async Task<bool> RetryConnectToCluster(string bootstrapServers, int maxRetries, int delayBetweenRetriesSeconds)
    {
        int retryCount = 0;
        bool isConnected = false;

        while (retryCount < maxRetries)
        {
            try
            {
                var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
                using var adminClient = new AdminClientBuilder(adminConfig).Build();
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                Console.WriteLine($"Conexion exitosa al cluster: {metadata.OriginatingBrokerName}");
                isConnected = true;
                break; // Salir del ciclo si la conexion es exitosa
            }
            catch (Exception ex)
            {
                retryCount++;
                Console.WriteLine($"Intentando conectar a Kafka... (Intento {retryCount}/{maxRetries})");
                Console.WriteLine($"Error al intentar conectar al cluster de Kafka. Reintentando en {delayBetweenRetriesSeconds} segundos...");
                if (retryCount >= maxRetries)
                {
                    Console.WriteLine("No se pudo conectar despues de varios intentos.");
                    break;
                }
                await Task.Delay(TimeSpan.FromSeconds(delayBetweenRetriesSeconds));
            }
        }

        return isConnected;
    }

    private static void ShowMetrics(object state)
    {
        var avgProcessingTime = _processedMessages > 0 ? _totalProcessingTimeMs / _processedMessages : 0;
        Console.WriteLine("===== METRICAS =====");
        Console.WriteLine($"Mensajes procesados: {_processedMessages}");
        Console.WriteLine($"Mensajes fallidos: {_failedMessages}");
        Console.WriteLine($"Mensajes descartados: {_discardedMessages}");
        Console.WriteLine($"Errores generales: {_generalErrors}");
        Console.WriteLine($"Intentos de reconexion: {_retryAttempts}");
        Console.WriteLine($"Tiempo promedio de procesamiento de mensajes (ms): {avgProcessingTime:F2}");
        Console.WriteLine("=====================");
    }

    private static async Task ProcessMessageAsync(string message)
    {
        await _semaphore.WaitAsync(); // Esperar a que haya un espacio disponible para procesar el mensaje

        try
        {
            Console.WriteLine($"Procesando mensaje: {message}");

            using var client = new HttpClient();
            client.BaseAddress = new Uri(Environment.GetEnvironmentVariable("SOU_BASE_URL") ?? "https://dev.api.strixlatam.com");

            // Serializar el mensaje como un JSON válido
            var content = new StringContent(JsonSerializer.Serialize(message), Encoding.UTF8, "application/json");

            // Enviar el mensaje a la API
            var response = await client.PostAsync("", content);

            if (response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Mensaje enviado con éxito. Respuesta: {await response.Content.ReadAsStringAsync()}");
            }
            else
            {
                Console.WriteLine($"Error al enviar el mensaje: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
            }
        }
        finally
        {
            _semaphore.Release(); // Liberar el espacio para que otras tareas puedan comenzar
        }
    }

    private static async Task<bool> HandleConsumeErrorAsync(ConsumeException ex, int maxRetries, int delayBetweenRetriesSeconds)
    {
        // Manejar error de tópico no disponible
        if (ex.Error.Reason.Contains("Unknown topic or partition"))
        {
            Interlocked.Increment(ref _failedMessages);

            for (int retryCount = 1; retryCount <= maxRetries; retryCount++)
            {
                Console.WriteLine($"Error al consumir mensajes: {ex.Error.Reason}");
                Console.WriteLine($"Tópico no disponible. Reintento {retryCount} de {maxRetries}...");

                // Esperar antes de reintentar
                await Task.Delay(TimeSpan.FromSeconds(delayBetweenRetriesSeconds));

                if (retryCount == maxRetries)
                {
                    Console.WriteLine("Se alcanzó el máximo de reintentos para el tópico. Finalizando aplicación...");
                    return true; // Finalizar proceso
                }
            }
        }

        // Manejar otros errores fatales
        if (ex.Error.IsFatal)
        {
            Interlocked.Increment(ref _failedMessages);

            for (int retryCount = 1; retryCount <= maxRetries; retryCount++)
            {
                Console.WriteLine($"Error fatal detectado: {ex.Error.Reason}");
                Console.WriteLine($"Reintento {retryCount} de {maxRetries}...");

                // Esperar antes de reintentar
                await Task.Delay(TimeSpan.FromSeconds(delayBetweenRetriesSeconds));

                if (retryCount == maxRetries)
                {
                    Console.WriteLine("Se alcanzó el máximo de reintentos tras un error fatal. Finalizando aplicación...");
                    return true; // Finalizar proceso
                }
            }
        }

        // Si no es un error fatal ni relacionado con el tópico, continuar
        Console.WriteLine($"Error al consumir mensajes: {ex.Error.Reason}");
        return false;
    }

}
