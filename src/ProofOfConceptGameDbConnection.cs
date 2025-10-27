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

public class ProofOfConceptGameDbConnection : SQLiteDBConnection, IGameDbConnection, IDisposable
{
    private SqliteCommand setChunksCmd;

    private SqliteCommand setMapChunksCmd;

    public override string DBTypeCode => "savegame database";

    public ProofOfConceptGameDbConnection(ILogger logger)
        : base(logger)
    {
        this.logger = logger;
    }

    public override void OnOpened()
    {
        this.setChunksCmd = this.sqliteConn.CreateCommand();
        this.setChunksCmd.CommandText = "INSERT OR REPLACE INTO chunk (position, data) VALUES (@position,@data)";
        this.setChunksCmd.Parameters.Add((object)this.CreateParameter("position", DbType.UInt64, (object)0, (DbCommand)this.setChunksCmd));
        this.setChunksCmd.Parameters.Add((object)this.CreateParameter("data", DbType.Object, (object)null, (DbCommand)this.setChunksCmd));
        this.setChunksCmd.Prepare();
        this.setMapChunksCmd = this.sqliteConn.CreateCommand();
        this.setMapChunksCmd.CommandText = "INSERT OR REPLACE INTO mapchunk (position, data) VALUES (@position,@data)";
        this.setMapChunksCmd.Parameters.Add((object)this.CreateParameter("position", DbType.UInt64, (object)0, (DbCommand)this.setMapChunksCmd));
        this.setMapChunksCmd.Parameters.Add((object)this.CreateParameter("data", DbType.Object, (object)null, (DbCommand)this.setMapChunksCmd));
        this.setMapChunksCmd.Prepare();
    }

    public void UpgradeToWriteAccess() => this.CreateTablesIfNotExists(this.sqliteConn);

    public bool IntegrityCheck()
    {
        if (!this.DoIntegrityCheck(this.sqliteConn))
        {
            string message = "Database integrity check failed. Attempt basic repair procedure (via VACUUM), this might take minutes to hours depending on the size of the save game...";
            this.logger.Notification(message);
            this.logger.StoryEvent(message);
            try
            {
                using (SqliteCommand command = this.sqliteConn.CreateCommand())
                {
                    command.CommandText = "PRAGMA writable_schema=ON;";
                    command.ExecuteNonQuery();
                }

                using (SqliteCommand command = this.sqliteConn.CreateCommand())
                {
                    command.CommandText = "VACUUM;";
                    command.ExecuteNonQuery();
                }
            }
            catch
            {
                this.logger.StoryEvent("Unable to repair :(");
                this.logger.Notification("Unable to repair :(\nRecommend any of the solutions posted here: https://wiki.vintagestory.at/index.php/Repairing_a_corrupt_savegame_or_worldmap\nWill exit now");
                throw new Exception("Database integrity bad");
            }

            if (this.DoIntegrityCheck(this.sqliteConn, false))
            {
                this.logger.Notification("Database integrity check now okay, yay!");
            }
            else
            {
                this.logger.StoryEvent("Unable to repair :(");
                this.logger.Notification("Database integrity still bad :(\nRecommend any of the solutions posted here: https://wiki.vintagestory.at/index.php/Repairing_a_corrupt_savegame_or_worldmap\nWill exit now");
                throw new Exception("Database integrity bad");
            }
        }

        return true;
    }

    public int QuantityChunks()
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT count(*) FROM chunk";
            return Convert.ToInt32(command.ExecuteScalar());
        }
    }

    public IEnumerable<DbChunk> GetAllChunks(string tablename)
    {
        using (SqliteCommand cmd = this.sqliteConn.CreateCommand())
        {
            cmd.CommandText = "SELECT position, data FROM " + tablename;
            SqliteDataReader sqlite_datareader = cmd.ExecuteReader();
            while (sqlite_datareader.Read())
            {
                object obj = sqlite_datareader["data"];
                ChunkPos chunkPos = ChunkPos.FromChunkIndex_saveGamev2((ulong)(long)sqlite_datareader["position"]);
                yield return new DbChunk()
                {
                    Position = chunkPos,
                    Data = obj as byte[]
                };
            }
        }
    }

    public IEnumerable<DbChunk> GetAllChunks() => this.GetAllChunks("chunk");

    public IEnumerable<DbChunk> GetAllMapChunks() => this.GetAllChunks("mapchunk");

    public IEnumerable<DbChunk> GetAllMapRegions() => this.GetAllChunks("mapregion");

    public void ForAllChunks(Action<DbChunk> action)
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT position, data FROM chunk";
            using (SqliteDataReader sqliteDataReader = command.ExecuteReader())
            {
                while (sqliteDataReader.Read())
                {
                    object obj = sqliteDataReader["data"];
                    ChunkPos chunkPos = ChunkPos.FromChunkIndex_saveGamev2((ulong)(long)sqliteDataReader["position"]);
                    action(new DbChunk()
                    {
                        Position = chunkPos,
                        Data = obj as byte[]
                    });
                }
            }
        }
    }

    public byte[] GetPlayerData(string playeruid)
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT data FROM playerdata WHERE playeruid=@playeruid";
            command.Parameters.Add((object)this.CreateParameter(nameof(playeruid), DbType.String, (object)playeruid, (DbCommand)command));
            using (SqliteDataReader sqliteDataReader = command.ExecuteReader())
                return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : (byte[])null;
        }
    }

    public void SetPlayerData(string playeruid, byte[] data)
    {
        if (data == null)
        {
            using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
            {
                command.CommandText = "DELETE FROM playerdata WHERE playeruid=@playeruid";
                command.Parameters.Add((object)this.CreateParameter(nameof(playeruid), DbType.String, (object)playeruid, command));
                command.ExecuteNonQuery();
            }
        }
        else if (this.GetPlayerData(playeruid) == null)
        {
            using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
            {
                command.CommandText = "INSERT INTO playerdata (playeruid, data) VALUES (@playeruid,@data)";
                command.Parameters.Add((object)this.CreateParameter(nameof(playeruid), DbType.String, (object)playeruid, command));
                command.Parameters.Add((object)this.CreateParameter(nameof(data), DbType.Object, (object)data, command));
                command.ExecuteNonQuery();
            }
        }
        else
        {
            using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
            {
                command.CommandText = "UPDATE playerdata set data=@data where playeruid=@playeruid";
                command.Parameters.Add((object)this.CreateParameter(nameof(data), DbType.Object, (object)data, command));
                command.Parameters.Add((object)this.CreateParameter(nameof(playeruid), DbType.String, (object)playeruid, command));
                command.ExecuteNonQuery();
            }
        }
    }

    public IEnumerable<byte[]> GetChunks(IEnumerable<ChunkPos> chunkpositions)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction transaction = this.sqliteConn.BeginTransaction())
            {
                IEnumerator<ChunkPos> enumerator = chunkpositions.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    ChunkPos current = enumerator.Current;
                    yield return this.GetChunk(current.ToChunkIndex(), "chunk");
                }

                enumerator = (IEnumerator<ChunkPos>)null;
                transaction.Commit();
            }
        }
    }

    public byte[] GetChunk(ulong position) => this.GetChunk(position, "chunk");

    public byte[] GetMapChunk(ulong position) => this.GetChunk(position, "mapchunk");

    public byte[] GetMapRegion(ulong position) => this.GetChunk(position, "mapregion");

    public bool ChunkExists(ulong position) => this.ChunkExists(position, "chunk");

    public bool MapChunkExists(ulong position) => this.ChunkExists(position, "mapchunk");

    public bool MapRegionExists(ulong position) => this.ChunkExists(position, "mapregion");

    public bool ChunkExists(ulong position, string tablename)
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = $"SELECT position FROM {tablename} WHERE position=@position";
            command.Parameters.Add((object)this.CreateParameter(nameof(position), DbType.UInt64, (object)position, (DbCommand)command));
            using (SqliteDataReader sqliteDataReader = command.ExecuteReader())
                return sqliteDataReader.HasRows;
        }
    }

    public byte[] GetChunk(ulong position, string tablename)
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = $"SELECT data FROM {tablename} WHERE position=@position";
            command.Parameters.Add((object)this.CreateParameter(nameof(position), DbType.UInt64, (object)position, (DbCommand)command));
            using (SqliteDataReader sqliteDataReader = command.ExecuteReader())
                return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : (byte[])null;
        }
    }

    public void DeleteChunks(IEnumerable<ChunkPos> chunkpositions)
    {
        this.DeleteChunks(chunkpositions, "chunk");
    }

    public void DeleteMapChunks(IEnumerable<ChunkPos> mapchunkpositions)
    {
        this.DeleteChunks(mapchunkpositions, "mapchunk");
    }

    public void DeleteMapRegions(IEnumerable<ChunkPos> mapchunkregions)
    {
        this.DeleteChunks(mapchunkregions, "mapregion");
    }

    public void DeleteChunks(IEnumerable<ChunkPos> chunkpositions, string tablename)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction sqliteTransaction = this.sqliteConn.BeginTransaction())
            {
                foreach (ChunkPos chunkposition in chunkpositions)
                    this.DeleteChunk(chunkposition.ToChunkIndex(), tablename);
                sqliteTransaction.Commit();
            }
        }
    }

    public void DeleteChunk(ulong position, string tablename)
    {
        using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
        {
            command.CommandText = $"DELETE FROM {tablename} WHERE position=@position";
            command.Parameters.Add((object)this.CreateParameter(nameof(position), DbType.UInt64, (object)position, command));
            command.ExecuteNonQuery();
        }
    }

    public void SetChunks(IEnumerable<DbChunk> chunks)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction sqliteTransaction = this.sqliteConn.BeginTransaction())
            {
                this.setChunksCmd.Transaction = sqliteTransaction;
                foreach (DbChunk chunk in chunks)
                {
                    this.setChunksCmd.Parameters["position"].Value = (object)chunk.Position.ToChunkIndex();
                    this.setChunksCmd.Parameters["data"].Value = (object)chunk.Data;
                    this.setChunksCmd.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public void SetMapChunks(IEnumerable<DbChunk> mapchunks)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction sqliteTransaction = this.sqliteConn.BeginTransaction())
            {
                this.setMapChunksCmd.Transaction = sqliteTransaction;
                foreach (DbChunk mapchunk in mapchunks)
                {
                    mapchunk.Position.Y = 0;
                    this.setMapChunksCmd.Parameters["position"].Value = (object)mapchunk.Position.ToChunkIndex();
                    this.setMapChunksCmd.Parameters["data"].Value = (object)mapchunk.Data;
                    this.setMapChunksCmd.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public void SetMapRegions(IEnumerable<DbChunk> mapregions)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction sqliteTransaction = this.sqliteConn.BeginTransaction())
            {
                foreach (DbChunk mapregion in mapregions)
                {
                    mapregion.Position.Y = 0;
                    this.InsertChunk(mapregion.Position.ToChunkIndex(), mapregion.Data, "mapregion");
                }

                sqliteTransaction.Commit();
            }
        }
    }

    private void InsertChunk(ulong position, byte[] data, string tablename)
    {
        using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
        {
            command.CommandText = $"INSERT OR REPLACE INTO {tablename} (position, data) VALUES (@position,@data)";
            command.Parameters.Add((object)this.CreateParameter(nameof(position), DbType.UInt64, (object)position, command));
            command.Parameters.Add((object)this.CreateParameter(nameof(data), DbType.Object, (object)data, command));
            command.ExecuteNonQuery();
        }
    }

    public byte[] GetGameData()
    {
        try
        {
            using (SqliteCommand command = this.sqliteConn.CreateCommand())
            {
                command.CommandText = "SELECT data FROM gamedata LIMIT 1";
                using (SqliteDataReader sqliteDataReader = command.ExecuteReader())
                    return sqliteDataReader.Read() ? sqliteDataReader["data"] as byte[] : (byte[])null;
            }
        }
        catch (Exception ex)
        {
            this.logger.Warning("Exception thrown on GetGlobalData: " + ex.Message);
            return (byte[])null;
        }
    }

    public void StoreGameData(byte[] data)
    {
        lock (this.transactionLock)
        {
            using (SqliteTransaction sqliteTransaction = this.sqliteConn.BeginTransaction())
            {
                using (DbCommand command = (DbCommand)this.sqliteConn.CreateCommand())
                {
                    command.CommandText = "INSERT OR REPLACE INTO gamedata (savegameid, data) VALUES (@savegameid,@data)";
                    command.Parameters.Add((object)this.CreateParameter("savegameid", DbType.UInt64, (object)1, command));
                    command.Parameters.Add((object)this.CreateParameter(nameof(data), DbType.Object, (object)data, command));
                    command.ExecuteNonQuery();
                }

                sqliteTransaction.Commit();
            }
        }
    }

    public bool QuickCorrectSaveGameVersionTest()
    {
        using (SqliteCommand command = this.sqliteConn.CreateCommand())
        {
            command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'gamedata';";
            return command.ExecuteScalar() != null;
        }
    }

    protected override void CreateTablesIfNotExists(SqliteConnection sqliteConn)
    {
        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS chunk (position integer PRIMARY KEY, data BLOB);";
            command.ExecuteNonQuery();
        }

        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS mapchunk (position integer PRIMARY KEY, data BLOB);";
            command.ExecuteNonQuery();
        }

        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS mapregion (position integer PRIMARY KEY, data BLOB);";
            command.ExecuteNonQuery();
        }

        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS gamedata (savegameid integer PRIMARY KEY, data BLOB);";
            command.ExecuteNonQuery();
        }

        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE TABLE IF NOT EXISTS playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);";
            command.ExecuteNonQuery();
        }

        using (SqliteCommand command = sqliteConn.CreateCommand())
        {
            command.CommandText = "CREATE index IF NOT EXISTS index_playeruid on playerdata(playeruid);";
            command.ExecuteNonQuery();
        }
    }

    public void CreateBackup(string backupFilename)
    {
        if (this.databaseFileName == backupFilename)
        {
            this.logger.Error("Cannot overwrite current running database. Chose another destination.");
        }
        else
        {
            if (File.Exists(backupFilename))
                this.logger.Error($"File {backupFilename} exists. Overwriting file.");
            SqliteConnection destination = new SqliteConnection(new DbConnectionStringBuilder()
            {
                {
                    "Data Source",
                    (object)Path.Combine(GamePaths.Backups, backupFilename)
                },
                {
                    "Pooling",
                    (object)"false"
                }
            }.ToString());
            destination.Open();
            using (SqliteCommand command = destination.CreateCommand())
            {
                command.CommandText = "PRAGMA journal_mode=Off;";
                command.ExecuteNonQuery();
            }

            this.sqliteConn.BackupDatabase(destination, destination.Database, this.sqliteConn.Database);
            destination.Close();
            destination.Dispose();
        }
    }

    public override void Close()
    {
        this.setChunksCmd?.Dispose();
        this.setMapChunksCmd?.Dispose();
        base.Close();
    }

    public override void Dispose()
    {
        this.setChunksCmd?.Dispose();
        this.setMapChunksCmd?.Dispose();
        base.Dispose();
    }
    
    bool IGameDbConnection.OpenOrCreate(
        string filename,
        ref string errorMessage,
        bool requireWriteAccess,
        bool corruptionProtection,
        bool doIntegrityCheck)
    {
        return this.OpenOrCreate(filename, ref errorMessage, requireWriteAccess, corruptionProtection, doIntegrityCheck);
    }

    void IGameDbConnection.Vacuum() => this.Vacuum();
}