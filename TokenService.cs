using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Logging;

namespace KafkaConsumer
{
    public static class TokenService
    {
        public static async Task<(string? token, DateTime expiration)> GetTokenAsync(
            string tokenUrl,
            string authorizationHeader,
            List<KeyValuePair<string, string>> parameters,
            ILogger? logger)
        {
            try
            {
                var client = new HttpClient();
                var request = new HttpRequestMessage(HttpMethod.Post, tokenUrl);
                request.Headers.Add("Authorization", $"Basic {authorizationHeader}");

                var content = new FormUrlEncodedContent(parameters);
                request.Content = content;

                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    var jsonResponse = JObject.Parse(responseContent);

                    var accessToken = jsonResponse.GetValue("access_token")?.ToString();
                    var expiresInValue = jsonResponse.GetValue("expires_in")?.ToString();

                    if (accessToken != null && expiresInValue != null && int.TryParse(expiresInValue, out int expiresIn))
                    {
                        var expiration = DateTime.Now.AddSeconds(expiresIn - 60); // Restar 60s por seguridad
                        return (accessToken, expiration);
                    }
                    else
                    {
                        logger.LogError("No se pudo obtener el token o la duracion del mismo.");
                        throw new Exception("No se pudo obtener el token o la duracion del mismo.");
                    }
                }

                return (null, DateTime.MinValue); // Si falla la autenticación
            }
            catch (Exception ex)
            {
                logger.LogError(ex.Message);
                return (null, DateTime.MinValue);
            }
        }
    }
}
