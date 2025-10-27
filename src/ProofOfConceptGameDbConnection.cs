using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using Microsoft.Data.Sqlite;
using Vintagestory.API.Common;
using Vintagestory.API.Config;
using Vintagestory.Common.Database;

namespace WorldSaveProofOfConcept;

public class ProofOfConceptGameDbConnection : SQLiteDBConnection, IGameDbConnection
{
    private SqliteCommand? _setChunksCmd;

    private SqliteCommand? _setMapChunksCmd;

    public override string DBTypeCode => "savegame database";

    public ProofOfConceptGameDbConnection(ILogger logger) : base(logger)
    {
        this.logger = logger;
    }

    public override void OnOpened()
    {
        _setChunksCmd = sqliteConn.CreateCommand();
        _setChunksCmd.CommandText = "INSERT OR REPLACE INTO chunk (position, data, last_updated) VALUES (@position, @data, unixepoch('now'))";
        _setChunksCmd.Parameters.Add(CreateParameter("position", DbType.UInt64, 0, _setChunksCmd));
        _setChunksCmd.Parameters.Add(CreateParameter("data", DbType.Object, null, _setChunksCmd));
        _setChunksCmd.Prepare();
        _setMapChunksCmd = sqliteConn.CreateCommand();
        _setMapChunksCmd.CommandText = "INSERT OR REPLACE INTO mapchunk (position, data, last_updated) VALUES (@position, @data, unixepoch('now'))";
        _setMapChunksCmd.Parameters.Add(CreateParameter("position", DbType.UInt64, 0, _setMapChunksCmd));
        _setMapChunksCmd.Parameters.Add(CreateParameter("data", DbType.Object, null, _setMapChunksCmd));
        _setMapChunksCmd.Prepare();
    }

    public void UpgradeToWriteAccess() => CreateTablesIfNotExists(sqliteConn);

    public bool IntegrityCheck()
    {
        if (!DoIntegrityCheck(sqliteConn))
        {
            var message = "Database integrity check failed. Attempt basic repair procedure (via VACUUM), this might take minutes to hours depending on the size of the save game...";
            logger.Notification(message);
            logger.StoryEvent(message);
            try
            {
                using (var command = sqliteConn.CreateCommand())
                {
                    command.CommandText = "PRAGMA writable_schema=ON;";
                    command.ExecuteNonQuery();
                }

                using (var command = sqliteConn.CreateCommand())
                {
                    command.CommandText = "VACUUM;";
                    command.ExecuteNonQuery();
                }
            }
            catch
            {
                logger.StoryEvent("Unable to repair :(");
                logger.Notification("Unable to repair :(\nRecommend any of the solutions posted here: https://wiki.vintagestory.at/index.php/Repairing_a_corrupt_savegame_or_worldmap\nWill exit now");
                throw new Exception("Database integrity bad");
            }

            if (DoIntegrityCheck(sqliteConn, false))
            {
                logger.Notification("Database integrity check now okay, yay!");
            }
            else
            {
                logger.StoryEvent("Unable to repair :(");
                logger.Notification("Database integrity still bad :(\nRecommend any of the solutions posted here: https://wiki.vintagestory.at/index.php/Repairing_a_corrupt_savegame_or_worldmap\nWill exit now");
                throw new Exception("Database integrity bad");
            }
        }

        return true;
    }

    public int QuantityChunks()
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT count(*) FROM chunk";
            return Convert.ToInt32(command.ExecuteScalar());
        }
    }

    public IEnumerable<DbChunk> GetAllChunks(string tableName)
    {
        using (var cmd = sqliteConn.CreateCommand())
        {
            cmd.CommandText = "SELECT position, data FROM " + tableName;
            var sqliteDataReader = cmd.ExecuteReader();
            while (sqliteDataReader.Read())
            {
                var obj = sqliteDataReader["data"];
                var chunkPos = ChunkPos.FromChunkIndex_saveGamev2((ulong)(long) sqliteDataReader["position"]);
                yield return new DbChunk
                {
                    Position = chunkPos,
                    Data = obj as byte[]
                };
            }
        }
    }

    public IEnumerable<DbChunk> GetAllChunks() => GetAllChunks("chunk");

    public IEnumerable<DbChunk> GetAllMapChunks() => GetAllChunks("mapchunk");

    public IEnumerable<DbChunk> GetAllMapRegions() => GetAllChunks("mapregion");

    public void ForAllChunks(Action<DbChunk> action)
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT position, data FROM chunk";
            using (var sqliteDataReader = command.ExecuteReader())
            {
                while (sqliteDataReader.Read())
                {
                    var obj = sqliteDataReader["data"];
                    var chunkPos = ChunkPos.FromChunkIndex_saveGamev2((ulong)(long) sqliteDataReader["position"]);
                    action(new DbChunk
                    {
                        Position = chunkPos,
                        Data = obj as byte[]
                    });
                }
            }
        }
    }

    public byte[]? GetPlayerData(string playeruid)
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT data FROM playerdata WHERE playeruid = @playeruid";
            command.Parameters.Add(CreateParameter("playeruid", DbType.String, playeruid, command));
            using (var sqliteDataReader = command.ExecuteReader())
            {
                return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : null;
            }
        }
    }

    public void SetPlayerData(string playeruid, byte[]? data)
    {
        if (data == null)
        {
            using (DbCommand command = sqliteConn.CreateCommand())
            {
                command.CommandText = "DELETE FROM playerdata WHERE playeruid = @playeruid";
                command.Parameters.Add(CreateParameter("playeruid", DbType.String, playeruid, command));
                command.ExecuteNonQuery();
            }
        }
        else if (GetPlayerData(playeruid) == null)
        {
            using (DbCommand command = sqliteConn.CreateCommand())
            {
                command.CommandText = "INSERT INTO playerdata (playeruid, data, unixepoch('now')) VALUES (@playeruid, @data)";
                command.Parameters.Add(CreateParameter("playeruid", DbType.String, playeruid, command));
                command.Parameters.Add(CreateParameter("data", DbType.Object, data, command));
                command.ExecuteNonQuery();
            }
        }
        else
        {
            using (DbCommand command = sqliteConn.CreateCommand())
            {
                command.CommandText = "UPDATE playerdata SET data = @data, last_updated = unixepoch('now') WHERE playeruid = @playeruid";
                command.Parameters.Add(CreateParameter("data", DbType.Object, data, command));
                command.Parameters.Add(CreateParameter("playeruid", DbType.String, playeruid, command));
                command.ExecuteNonQuery();
            }
        }
    }

    public IEnumerable<byte[]?> GetChunks(IEnumerable<ChunkPos> chunkPositions)
    {
        lock (transactionLock)
        {
            using (var transaction = sqliteConn.BeginTransaction())
            {
                foreach (var chunkPose in chunkPositions)
                {
                    yield return GetChunk(chunkPose.ToChunkIndex(), "chunk");
                }
                transaction.Commit();
            }
        }
    }

    public byte[]? GetChunk(ulong position) => GetChunk(position, "chunk");

    public byte[]? GetMapChunk(ulong position) => GetChunk(position, "mapchunk");

    public byte[]? GetMapRegion(ulong position) => GetChunk(position, "mapregion");

    public bool ChunkExists(ulong position) => ChunkExists(position, "chunk");

    public bool MapChunkExists(ulong position) => ChunkExists(position, "mapchunk");

    public bool MapRegionExists(ulong position) => ChunkExists(position, "mapregion");

    public bool ChunkExists(ulong position, string tableName)
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = $"SELECT position FROM {tableName} WHERE position = @position";
            command.Parameters.Add(CreateParameter("position", DbType.UInt64, position, command));
            using (var sqliteDataReader = command.ExecuteReader())
            {
                return sqliteDataReader.HasRows;
            }
        }
    }

    public byte[]? GetChunk(ulong position, string tableName)
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = $"SELECT data FROM {tableName} WHERE position = @position";
            command.Parameters.Add(CreateParameter("position", DbType.UInt64, position, command));
            using (var sqliteDataReader = command.ExecuteReader())
            {
                return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : null;
            }
        }
    }

    public void DeleteChunks(IEnumerable<ChunkPos> chunkPositions)
    {
        DeleteChunks(chunkPositions, "chunk");
    }

    public void DeleteMapChunks(IEnumerable<ChunkPos> mapchunkpositions)
    {
        DeleteChunks(mapchunkpositions, "mapchunk");
    }

    public void DeleteMapRegions(IEnumerable<ChunkPos> mapchunkregions)
    {
        DeleteChunks(mapchunkregions, "mapregion");
    }

    public void DeleteChunks(IEnumerable<ChunkPos> chunkPositions, string tableName)
    {
        lock (transactionLock)
        {
            using (var sqliteTransaction = sqliteConn.BeginTransaction())
            {
                foreach (var chunkPosition in chunkPositions)
                {
                    DeleteChunk(chunkPosition.ToChunkIndex(), tableName);
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public void DeleteChunk(ulong position, string tableName)
    {
        using (DbCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = $"DELETE FROM {tableName} WHERE position = @position";
            command.Parameters.Add(CreateParameter("position", DbType.UInt64, position, command));
            command.ExecuteNonQuery();
        }
    }

    public void SetChunks(IEnumerable<DbChunk> chunks)
    {
        lock (transactionLock)
        {
            using (var sqliteTransaction = sqliteConn.BeginTransaction())
            {
                _setChunksCmd!.Transaction = sqliteTransaction; // Possibly null reference suppressed to match vanilla code.
                foreach (var chunk in chunks)
                {
                    _setChunksCmd.Parameters["position"].Value = chunk.Position.ToChunkIndex();
                    _setChunksCmd.Parameters["data"].Value = chunk.Data;
                    _setChunksCmd.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public void SetMapChunks(IEnumerable<DbChunk> mapchunks)
    {
        lock (transactionLock)
        {
            using (var sqliteTransaction = sqliteConn.BeginTransaction())
            {
                _setMapChunksCmd!.Transaction = sqliteTransaction; // Possibly null reference suppressed to match vanilla code.
                foreach (var mapChunk in mapchunks)
                {
                    mapChunk.Position.Y = 0;
                    _setMapChunksCmd.Parameters["position"].Value = mapChunk.Position.ToChunkIndex();
                    _setMapChunksCmd.Parameters["data"].Value = mapChunk.Data;
                    _setMapChunksCmd.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public void SetMapRegions(IEnumerable<DbChunk> mapregions)
    {
        lock (transactionLock)
        {
            using (var sqliteTransaction = sqliteConn.BeginTransaction())
            {
                foreach (var mapregion in mapregions)
                {
                    mapregion.Position.Y = 0;
                    InsertChunk(mapregion.Position.ToChunkIndex(), mapregion.Data, "mapregion");
                }

                sqliteTransaction.Commit();
            }
        }
    }

    private void InsertChunk(ulong position, byte[] data, string tableName)
    {
        using (DbCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = $"INSERT OR REPLACE INTO {tableName} (position, data, last_updated) VALUES (@position, @data, unixepoch('now'))";
            command.Parameters.Add(CreateParameter("position", DbType.UInt64, position, command));
            command.Parameters.Add(CreateParameter("data", DbType.Object, data, command));
            command.ExecuteNonQuery();
        }
    }

    public byte[]? GetGameData()
    {
        try
        {
            using (var command = sqliteConn.CreateCommand())
            {
                command.CommandText = "SELECT data FROM gamedata LIMIT 1";
                using (var sqliteDataReader = command.ExecuteReader())
                {
                    return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : null;
                }
            }
        }
        catch (Exception ex)
        {
            logger.Warning("Exception thrown on GetGlobalData: " + ex.Message);
            return null;
        }
    }

    public void StoreGameData(byte[] data)
    {
        lock (transactionLock)
        {
            using (var sqliteTransaction = sqliteConn.BeginTransaction())
            {
                using (DbCommand command = sqliteConn.CreateCommand())
                {
                    command.CommandText = "INSERT OR REPLACE INTO gamedata (savegameid, data) VALUES (@savegameid, @data)";
                    command.Parameters.Add(CreateParameter("savegameid", DbType.UInt64, 1, command));
                    command.Parameters.Add(CreateParameter("data", DbType.Object, data, command));
                    command.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public bool QuickCorrectSaveGameVersionTest()
    {
        using (var command = sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'gamedata';";
            return command.ExecuteScalar() != null;
        }
    }

    protected override void CreateTablesIfNotExists(SqliteConnection sqliteConnection)
    {
        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS chunk (position integer PRIMARY KEY, data BLOB, last_updated INTEGER);";
            command.ExecuteNonQuery();
        }

        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS mapchunk (position integer PRIMARY KEY, data BLOB, last_updated INTEGER);";
            command.ExecuteNonQuery();
        }

        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS mapregion (position integer PRIMARY KEY, data BLOB, last_updated INTEGER);";
            command.ExecuteNonQuery();
        }

        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS gamedata (savegameid integer PRIMARY KEY, data BLOB, last_updated INTEGER);";
            command.ExecuteNonQuery();
        }

        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB, last_updated INTEGER);";
            command.ExecuteNonQuery();
        }

        using (var command = sqliteConnection.CreateCommand())
        {
            command.CommandText = "CREATE INDEX IF NOT EXISTS index_playeruid on playerdata(playeruid);";
            command.ExecuteNonQuery();
        }
        
        CreateLastUpdatedColumnIfNotExists(sqliteConnection);
    }

    public void CreateLastUpdatedColumnIfNotExists() => CreateLastUpdatedColumnIfNotExists(sqliteConn);
    private void CreateLastUpdatedColumnIfNotExists(SqliteConnection sqliteConnection)
    {
        string[] lastUpdatedTables = ["chunk", "mapchunk", "mapregion", "playerdata"];
        foreach (var tableName in lastUpdatedTables)
        {
            using (var command = sqliteConnection.CreateCommand())
            {
                command.CommandText = $"SELECT COUNT(*) FROM pragma_table_info('{tableName}') WHERE name='last_updated';";
                if ((long) command.ExecuteScalar()! > 0)
                {
                    logger.Notification($"Found existing last_updated column on {tableName}");
                    // Already has last_updated column.
                    continue;
                }
            }
            
            logger.Notification($"Adding last_updated column to {tableName}");
            using (var command = sqliteConnection.CreateCommand())
            {
                command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN last_updated INTEGER;";
                command.ExecuteNonQuery();
            }

            logger.Notification($"Adding last_updated index to {tableName}");
            using (var indexCmd = sqliteConnection.CreateCommand())
            {
                indexCmd.CommandText = $"CREATE INDEX IF NOT EXISTS index_{tableName}_last_updated ON {tableName}(last_updated);";
                indexCmd.ExecuteNonQuery();
            }
        }
    }

    public void CreateBackup(string backupFilename)
    {
        if (databaseFileName == backupFilename)
        {
            logger.Error("Cannot overwrite current running database. Chose another destination.");
        }
        else
        {
            if (File.Exists(backupFilename))
                logger.Error($"File {backupFilename} exists. Overwriting file.");
            var destination = new SqliteConnection(new DbConnectionStringBuilder
            {
                {
                    "Data Source",
                    Path.Combine(GamePaths.Backups, backupFilename)
                },
                {
                    "Pooling",
                    "false"
                }
            }.ToString());
            destination.Open();
            using (var command = destination.CreateCommand())
            {
                command.CommandText = "PRAGMA journal_mode=Off;";
                command.ExecuteNonQuery();
            }

            sqliteConn.BackupDatabase(destination, destination.Database, sqliteConn.Database);
            destination.Close();
            destination.Dispose();
        }
    }

    public override void Close()
    {
        _setChunksCmd?.Dispose();
        _setMapChunksCmd?.Dispose();
        base.Close();
    }

    public override void Dispose()
    {
        _setChunksCmd?.Dispose();
        _setMapChunksCmd?.Dispose();
        base.Dispose();
    }

    bool IGameDbConnection.OpenOrCreate(
        string filename,
        ref string errorMessage,
        bool requireWriteAccess,
        bool corruptionProtection,
        bool doIntegrityCheck)
    {
        return OpenOrCreate(filename, ref errorMessage, requireWriteAccess, corruptionProtection, doIntegrityCheck);
    }

    void IGameDbConnection.Vacuum() => Vacuum();
}