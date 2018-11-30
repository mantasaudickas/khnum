namespace Khnum.Contracts
{
    public interface ISerializer
    {
        string Serialize<T>(T message);
        T Deserialize<T>(string content);
    }
}
