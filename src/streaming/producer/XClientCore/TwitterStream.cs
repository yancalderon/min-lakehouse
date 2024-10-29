using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TwitterClient
{
    internal static class TwitterStream
    {
        private static readonly HttpClient httpClient = new HttpClient();
        
        public static IEnumerable<string> StreamStatuses(TwitterConfig config)
        {
            var streamReader = ReadTweetsAsync(config).GetAwaiter().GetResult();
            if (streamReader == StreamReader.Null)
            {
                throw new Exception("Could not connect to X API with credentials provided");
            }

            while (true)
            {
                string line = null;
                try
                {
                    line = streamReader.ReadLine();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Ignoring: {e}");
                }

                if (!string.IsNullOrWhiteSpace(line))
                {
                    yield return line;
                }

                if (line == null)
                {
                    streamReader = ReadTweetsAsync(config).GetAwaiter().GetResult();
                }
            }
        }

        private static async Task<TextReader> ReadTweetsAsync(TwitterConfig config)
        {
            try
            {
                var resource_url = "https://api.twitter.com/2/tweets/search/stream";
                await SetupStreamRules(config);

                var request = new HttpRequestMessage(HttpMethod.Get, resource_url);
                
                var bearerToken = !string.IsNullOrEmpty(config.BearerToken) 
                    ? config.BearerToken 
                    : await GetBearerTokenAsync(config);
                
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
                request.RequestUri = new Uri($"{resource_url}?tweet.fields=created_at,author_id,text");

                var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
                
                if (response.IsSuccessStatusCode)
                {
                    var stream = await response.Content.ReadAsStreamAsync();
                    return new StreamReader(stream);
                }
                
                Console.WriteLine($"Error: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                return StreamReader.Null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error connecting to stream: {ex.Message}");
                return StreamReader.Null;
            }
        }

        private static async Task<string> GetBearerTokenAsync(TwitterConfig config)
        {
            var tokenEndpoint = "https://api.twitter.com/oauth2/token";
            
            var credentials = Convert.ToBase64String(
                Encoding.ASCII.GetBytes($"{config.OAuthConsumerKey}:{config.OAuthConsumerSecret}"));

            var request = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint);
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                { "grant_type", "client_credentials" }
            });

            var response = await httpClient.SendAsync(request);
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                var token = JsonSerializer.Deserialize<JsonElement>(content);
                return token.GetProperty("access_token").GetString();
            }

            throw new Exception("Failed to obtain Bearer token");
        }

        // Modificación en TwitterStream.cs
        private static async Task SetupStreamRules(TwitterConfig config)
        {
            var rulesEndpoint = "https://api.twitter.com/2/tweets/search/stream/rules";

            // Usar directamente el Bearer Token proporcionado
            if (string.IsNullOrEmpty(config.BearerToken))
            {
                throw new Exception("Bearer Token is required for Twitter API v2");
            }

            try
            {
                // Convertir las keywords en reglas válidas para la API v2
                var keywords = config.Keywords.Split(',')
                    .Select(k => k.Trim())
                    .Select(k => new { value = k })
                    .ToArray();

                var rules = new { add = keywords };

                var request = new HttpRequestMessage(HttpMethod.Post, rulesEndpoint);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", config.BearerToken);
                request.Content = new StringContent(
                    JsonSerializer.Serialize(rules),
                    Encoding.UTF8,
                    "application/json"
                );

                var response = await httpClient.SendAsync(request);
                var content = await response.Content.ReadAsStringAsync();

                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Error setting up rules: {content}");
                    throw new Exception($"Failed to add stream rules: {content}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in SetupStreamRules: {ex.Message}");
                throw;
            }
        }

        private static async Task<dynamic> GetExistingRules(string rulesEndpoint, string bearerToken)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, rulesEndpoint);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

            var response = await httpClient.SendAsync(request);
            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                return JsonSerializer.Deserialize<dynamic>(content);
            }

            return null;
        }

        private static async Task DeleteAllRules(string rulesEndpoint, dynamic existingRules, string bearerToken)
        {
            var ids = new List<string>();
            foreach (var rule in existingRules.data)
            {
                ids.Add(rule.id.ToString());
            }

            var deleteRequest = new
            {
                delete = new { ids }
            };

            var request = new HttpRequestMessage(HttpMethod.Post, rulesEndpoint);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);
            request.Content = new StringContent(JsonSerializer.Serialize(deleteRequest), Encoding.UTF8, "application/json");

            await httpClient.SendAsync(request);
        }
    }
}