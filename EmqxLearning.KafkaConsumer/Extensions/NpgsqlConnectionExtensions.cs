using Npgsql;
using NpgsqlTypes;

namespace EmqxLearning.KafkaConsumer.Extensions;

public static class NpgsqlConnectionExtensions
{
    public delegate void WriteRow<T>(T record, NpgsqlBinaryImporter writer);

    public static async Task BulkCopyFromAsync<T>(this NpgsqlConnection connection,
        string table,
        IEnumerable<string> columns,
        IEnumerable<T> records,
        WriteRow<T> WriteRow)
    {
        using (NpgsqlBinaryImporter writer = connection.BeginBinaryImport($"COPY {table} ({string.Join(',', columns)}) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (T record in records)
            {
                writer.StartRow();
                WriteRow(record, writer);
            }
            await writer.CompleteAsync();
        }
    }

    public static void WriteNullable<T>(this NpgsqlBinaryImporter writer, T value, NpgsqlDbType? type = null)
    {
        if (value == null)
        {
            writer.WriteNull();
        }
        else if (type.HasValue)
        {
            writer.Write(value, type.Value);
        }
        else
        {
            writer.Write(value);
        }
    }
}