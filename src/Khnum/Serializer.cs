using System.Text;
using Khnum.Contracts;
using Utf8Json;

namespace Khnum
{
    public class Serializer: ISerializer
    {
        public string Serialize<T>(T message)
        {
            return Encoding.UTF8.GetString(JsonSerializer.Serialize(message));
        }

        public T Deserialize<T>(string content)
        {
            if (string.IsNullOrWhiteSpace(content))
                return default(T);

            return JsonSerializer.Deserialize<T>(content);
        }
    }
}
