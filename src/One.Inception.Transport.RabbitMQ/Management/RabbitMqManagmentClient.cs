using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using One.Inception.Transport.RabbitMQ.Management.Model;

namespace One.Inception.Transport.RabbitMQ.Management;

public sealed class RabbitMqManagementClient : IDisposable
{
    private static readonly Regex UrlRegex = new Regex(@"^(http|https):\/\/.+\w$");

    readonly string username;
    readonly string password;
    readonly int portNumber;
    readonly bool useSsl;
    readonly int sslEnabledPort = 443;
    readonly int sslDisabledPort = 15672;
    readonly JsonSerializerOptions settings;

    readonly TimeSpan defaultTimeout = TimeSpan.FromSeconds(20);
    readonly TimeSpan timeout;

    private readonly List<string> apiAddressCollection;
    private string lastKnownApiAddress;
    private readonly HttpClient httpClient;

    public RabbitMqManagementClient(IRabbitMqOptions settings) : this(settings.ApiAddress ?? settings.Server, settings.Username, settings.Password, useSsl: settings.UseSsl) { }

    public RabbitMqManagementClient(string apiAddresses, string username, string password, bool useSsl = false, TimeSpan? timeout = null, Action<HttpClient> configureClient = null)
    {
        this.portNumber = useSsl ? sslEnabledPort : sslDisabledPort;
        this.useSsl = useSsl;
        this.apiAddressCollection = new List<string>();
        string[] parsedAddresses = apiAddresses.Split(',', StringSplitOptions.RemoveEmptyEntries);
        foreach (var apiAddress in parsedAddresses)
        {
            TryInitializeApiHostName(apiAddress, useSsl);
        }
        if (apiAddressCollection.Any() == false) throw new ArgumentException("Invalid API addresses", nameof(apiAddresses));

        if (string.IsNullOrEmpty(username)) throw new ArgumentException("username is null or empty");
        if (string.IsNullOrEmpty(password)) throw new ArgumentException("password is null or empty");

        this.username = username;
        this.password = password;

        this.timeout = timeout ?? defaultTimeout;
        this.settings = new JsonSerializerOptions
        {
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = true
        };

        var handler = new HttpClientHandler
        {
            Credentials = new System.Net.NetworkCredential(username, password),
            PreAuthenticate = true
        };

        httpClient = new HttpClient(handler)
        {
            Timeout = this.timeout
        };

        var authToken = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authToken);

        configureClient?.Invoke(httpClient);
    }

    private void TryInitializeApiHostName(string address, bool useSsl)
    {
        string result = $"{address.Trim()}:{portNumber}";

        if (string.IsNullOrEmpty(result)) return;

        string schema = useSsl ? "https://" : "http://";

        result = result.Contains(schema) ? result : schema + result;

        if (UrlRegex.IsMatch(result) && Uri.TryCreate(result, UriKind.Absolute, out _))
        {
            apiAddressCollection.Add(result);
        }
    }

    public async Task<Vhost> CreateVirtualHostAsync(string virtualHostName)
    {
        if (string.IsNullOrEmpty(virtualHostName)) throw new ArgumentException("virtualHostName is null or empty");

        await PutAsync($"vhosts/{virtualHostName}").ConfigureAwait(false);

        return await GetVhostAsync(virtualHostName).ConfigureAwait(false);
    }

    public async Task<Vhost> GetVhostAsync(string vhostName)
    {
        string vhost = SanitiseVhostName(vhostName);

        return await GetAsync<Vhost>($"vhosts/{vhost}").ConfigureAwait(false);
    }

    public async Task<IEnumerable<Vhost>> GetVHostsAsync()
    {
        return await GetAsync<IEnumerable<Vhost>>("vhosts").ConfigureAwait(false);
    }


    public async Task CreatePermissionAsync(PermissionInfo permissionInfo)
    {
        if (permissionInfo is null) throw new ArgumentNullException("permissionInfo");

        string vhost = SanitiseVhostName(permissionInfo.GetVirtualHostName());
        string username = permissionInfo.GetUserName();
        await PutAsync($"permissions/{vhost}/{username}", permissionInfo).ConfigureAwait(false);
    }

    public async Task CreateFederatedExchangeAsync(FederatedExchange exchange, string ownerVhost)
    {
        await PutAsync($"parameters/federation-upstream/{ownerVhost}/{exchange.Name}", exchange).ConfigureAwait(false);
    }

    public async Task CreatePolicyAsync(Policy policy, string ownerVhost)
    {
        await PutAsync($"policies/{ownerVhost}/{policy.Name}", policy).ConfigureAwait(false);
    }

    public async Task<IEnumerable<User>> GetUsersAsync()
    {
        return await GetAsync<IEnumerable<User>>("users").ConfigureAwait(false);
    }

    public async Task<User> GetUserAsync(string userName)
    {
        return await GetAsync<User>(string.Format("users/{0}", userName)).ConfigureAwait(false);
    }

    public async Task<User> CreateUserAsync(UserInfo userInfo)
    {
        if (userInfo is null) throw new ArgumentNullException("userInfo");

        string username = userInfo.GetName();

        await PutAsync($"users/{username}", userInfo).ConfigureAwait(false);

        return await GetUserAsync(userInfo.GetName()).ConfigureAwait(false);
    }

    private async Task PutAsync(string path)
    {
        var uri = await BuildEndpointUriAsync(path).ConfigureAwait(false);

        using var request = new HttpRequestMessage(HttpMethod.Put, uri);
        request.Content = new StringContent(string.Empty, Encoding.UTF8, "application/json");

        using var response = await httpClient.SendAsync(request).ConfigureAwait(false);

        // The "Cowboy" server in 3.7.0's Management Client returns 201 Created.
        // "MochiWeb/1.1 WebMachine/1.10.0 (never breaks eye contact)" in 3.6.1 and previous return 204 No Content
        // Also acceptable for a PUT response is 200 OK
        // See also http://stackoverflow.com/questions/797834/should_a_restful_put_operation_return_something
        if (!(response.StatusCode == System.Net.HttpStatusCode.OK ||
         response.StatusCode == System.Net.HttpStatusCode.Created ||
            response.StatusCode == System.Net.HttpStatusCode.NoContent))
        {
            throw new UnexpectedHttpStatusCodeException(response.StatusCode);
        }
    }

    private async Task PutAsync<T>(string path, T item)
    {
        var uri = await BuildEndpointUriAsync(path).ConfigureAwait(false);

        var json = JsonSerializer.Serialize(item, settings);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        using var response = await httpClient.PutAsync(uri, content).ConfigureAwait(false);

        // The "Cowboy" server in 3.7.0's Management Client returns 201 Created.
        // "MochiWeb/1.1 WebMachine/1.10.0 (never breaks eye contact)" in 3.6.1 and previous return 204 No Content
        // Also acceptable for a PUT response is 200 OK
        // See also http://stackoverflow.com/questions/797834/should_a_restful_put_operation_return_something
        if (!(response.StatusCode == System.Net.HttpStatusCode.OK ||
              response.StatusCode == System.Net.HttpStatusCode.Created ||
         response.StatusCode == System.Net.HttpStatusCode.NoContent))
        {
            throw new UnexpectedHttpStatusCodeException(response.StatusCode);
        }
    }

    private async Task<T> GetAsync<T>(string path, params object[] queryObjects)
    {
        var queryString = BuildQueryString(queryObjects);
        var fullPath = path + queryString;
        var uri = await BuildEndpointUriAsync(fullPath).ConfigureAwait(false);

        using var response = await httpClient.GetAsync(uri).ConfigureAwait(false);

        if (response.StatusCode != System.Net.HttpStatusCode.OK)
        {
            throw new UnexpectedHttpStatusCodeException(response.StatusCode);
        }

        var content = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        return JsonSerializer.Deserialize<T>(content, settings);
    }

    private string SanitiseVhostName(string vhostName) => Uri.EscapeDataString(vhostName);

    private async Task<Uri> BuildEndpointUriAsync(string path)
    {
        if (!string.IsNullOrEmpty(lastKnownApiAddress) && await IsHostRespondingAsync(lastKnownApiAddress))
        {
            return new Uri($"{lastKnownApiAddress}/api/{path}");
        }

        foreach (var apiAddress in apiAddressCollection)
        {
            if (await IsHostRespondingAsync(apiAddress).ConfigureAwait(false))
            {
                lastKnownApiAddress = apiAddress;
                return new Uri($"{apiAddress}/api/{path}");
            }
        }

        throw new Exception("Unable to connect to any of the provided API hosts.");
    }

    private async Task<bool> IsHostRespondingAsync(string address)
    {
        try
        {
            using var testClient = new HttpClient();
            var response = await testClient.GetAsync(address);
            return response.StatusCode == System.Net.HttpStatusCode.OK;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private string BuildQueryString(object[] queryObjects)
    {
        if (queryObjects == null || queryObjects.Length == 0)
            return string.Empty;

        StringBuilder queryStringBuilder = new StringBuilder("?");
        var first = true;
        // One or more query objects can be used to build the query
        foreach (var query in queryObjects)
        {
            if (query == null)
                continue;
            // All public properties are added to the query on the format property_name=value
            var type = query.GetType();
            foreach (var prop in type.GetProperties())
            {
                var name = Regex.Replace(prop.Name, "([a-z])([A-Z])", "$1_$2").ToLower();
                var value = prop.GetValue(query, null);
                if (!first)
                {
                    queryStringBuilder.Append("&");
                }
                queryStringBuilder.AppendFormat("{0}={1}", Uri.EscapeDataString(name), Uri.EscapeDataString(value?.ToString() ?? string.Empty));
                first = false;
            }
        }
        return queryStringBuilder.ToString();
    }

    public void Dispose()
    {
        httpClient?.Dispose();
    }
}
