using System.Text;
using Microsoft.Extensions.Logging;

namespace KafkaConsumer
{
    public class SOUService : ISOUService
    {
        private readonly HttpClient _httpClient;
        private readonly ILogger<SOUService> _logger;
        private string? _token;
        private DateTime _tokenExpirationTime;
        private readonly SemaphoreSlim _semaphore = new(10); // Limitar concurrencia a 10 solicitudes simultáneas

        private static readonly List<string> _failedMessagesQueue = new();
        private static readonly object _queueLock = new();

        public SOUService(ILogger<SOUService> logger)
        {
            _logger = logger;
            // Configura HttpClient para usar conexiones persistentes y optimización de la capa de transporte
            _httpClient = new HttpClient(new SocketsHttpHandler
            {
                PooledConnectionLifetime = TimeSpan.FromMinutes(5), // Mantener conexiones activas por 5 minutos
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2), // Tiempo máximo de inactividad de la conexión
                MaxConnectionsPerServer = 10 // Limita las conexiones simultáneas al servidor
            })
            {
                Timeout = TimeSpan.FromSeconds(int.Parse(Environment.GetEnvironmentVariable("SEND_TIMEOUT") ?? "60")) // Timeout general de las solicitudes
            };
        }

        public async Task<string> GetTokenAsync()
        {
            if (_token != null && DateTime.Now < _tokenExpirationTime)
            {
                return _token;
            }

            var authorizationHeader = Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1")
                .GetBytes($"{Environment.GetEnvironmentVariable("SOU_CLIENT_ID")}:{Environment.GetEnvironmentVariable("SOU_CLIENT_SECRET")}"));

            var parameters = new List<KeyValuePair<string, string>>
        {
            new("scope", "all"),
            new("grant_type", "client_credentials")
        };

            var result = await TokenService.GetTokenAsync(
                $"{Environment.GetEnvironmentVariable("SOU_BASE_URL")}/oauth/token",
                authorizationHeader,
                parameters,
                _logger
            );

            _token = result.token;
            _tokenExpirationTime = result.expiration;

            if (!string.IsNullOrEmpty(_token))
            {
                _logger.LogInformation("Nuevo token de SOU obtenido y almacenado en memoria.");
                return _token;
            }
            else
            {
                return string.Empty;
            }
        }

        // Método para obtener la cuenta
        public async Task SendSOUAsync(string param)
        {
            await _semaphore.WaitAsync(); // Limita la concurrencia de las solicitudes

            try
            {
                var maxRetries = int.Parse(Environment.GetEnvironmentVariable("RECONNECT_MAX_TRIES") ?? "3");
                var attempt = 0;
                bool success = false;

                while (attempt < maxRetries && !success)
                {
                    try
                    {
                        // Verifica si el token es válido
                        if (string.IsNullOrEmpty(_token) || DateTime.Now >= _tokenExpirationTime)
                        {
                            // Obtiene un nuevo token
                            await GetTokenAsync();
                        }

                        var content = new StringContent(param, null, "application/json");
                        _httpClient.DefaultRequestHeaders.Remove("Authorization"); // Asegúrate de eliminar el encabezado anterior
                        _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_token}");

                        var random = new Random();
                        bool simulateFailure = false; //random.Next(0, 2) == 0; // Genera un 50% de probabilidad de fallo

                        if (simulateFailure)
                        {
                            throw new Exception("Error simulado en el envío a la API externa.");
                        }

                        var response = await _httpClient.PostAsync($"{Environment.GetEnvironmentVariable("SOU_BASE_URL")}/sou/flotas/alertas", content);

                        if (response.IsSuccessStatusCode)
                        {
                            var result = await response.Content.ReadAsStringAsync();
                            _logger.LogInformation($"[SEND] param - [RESPONSE] {response.StatusCode} - {DateTime.Now}");
                            success = true; // El envío fue exitoso, no hay necesidad de reintentar
                        }
                        else
                        {
                            _logger.LogError($"[SEND]: param - [RESPONSE] {response.StatusCode} - {DateTime.Now}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error al procesar el item: param - Intento {attempt + 1}. {ex.Message}");
                    }

                    attempt++;

                    if (!success && attempt < maxRetries)
                    {
                        int seconds = int.Parse(Environment.GetEnvironmentVariable("RECONNECT_WAIT_SECONDS") ?? "15");
                        _logger.LogInformation($"Reintentando en {seconds} segundos... (Intento {attempt + 1}/{maxRetries})");
                        await Task.Delay(TimeSpan.FromSeconds(seconds)); // Espera antes del siguiente intento
                    }
                }

                if (!success)
                {
                    lock (_queueLock)
                    {
                        _failedMessagesQueue.Add(param);
                    }
                    _logger.LogError($"El evento {param} fue agregado a la lista de mensajes fallidos.");
                }
            }
            finally
            {
                _semaphore.Release();
            }
        }

        public async Task RetryFailedMessagesAsync()
        {
            List<string> retryMessages;

            // Extraer mensajes pendientes en un bloque protegido
            lock (_queueLock)
            {
                if (_failedMessagesQueue.Count == 0) return; // No hay mensajes para procesar
                retryMessages = new List<string>(_failedMessagesQueue);
                _failedMessagesQueue.Clear(); // Vaciar la lista para evitar reintentos duplicados
            }

            foreach (var message in retryMessages)
            {
                try
                {
                    await SendSOUAsync(message);
                    _logger.LogInformation($"Mensaje reenviado exitosamente: {message}");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error al reintentar el envío de mensaje: {message}. {ex.Message}");

                    // Si vuelve a fallar, reintegrarlo a la cola
                    lock (_queueLock)
                    {
                        _failedMessagesQueue.Add(message);
                    }
                }
            }
        }

    }
}
