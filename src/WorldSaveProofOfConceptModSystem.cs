using System;
using System.Reflection;
using Microsoft.Data.Sqlite;
using Vintagestory.API.Common;
using Vintagestory.API.Server;
using Vintagestory.Common;
using Vintagestory.Common.Database;
using Vintagestory.Server;

[assembly: ModInfo("Incremental World Backups", "incrementalworldbackups",
    Authors = [ "Cecer" ],
    Description = "Adds a small amount of additional data to the save file to allow for incremental backups",
    Version = "1.0.0")]

namespace IncrementalWorldBackups;

public class IncrementalWorldBackupsModSystem : ModSystem
{
    private HarmonyLib.Harmony? _harmony;
    private ICoreServerAPI _api = null!;
    
    public override bool ShouldLoad(EnumAppSide forSide) => forSide == EnumAppSide.Server;

    public override void StartServerSide(ICoreServerAPI api)
    {
        _api = api;
        
        _harmony = new HarmonyLib.Harmony("incrementalworldbackups");
        _harmony.PatchAll();
        CreateLastUpdatedColumnIfNotExists();
    }

    public override void Dispose()
    {
        _harmony?.UnpatchAll("incrementalworldbackups");
        _harmony = null;
    }

    private void CreateLastUpdatedColumnIfNotExists()
    {
        var serverMain = (ServerMain) typeof(ServerProgram).GetField("server", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;
        var chunkThread = (ChunkServerThread) serverMain.GetType().GetField("chunkThread", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(serverMain)!;
        var database = (GameDatabase) chunkThread.GetType().GetField("gameDatabase", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(chunkThread)!;
        var connField = database.GetType().GetField("conn", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var conn = (IGameDbConnection) connField.GetValue(database)!;
        var sqliteConnField = conn.GetType().GetField("sqliteConn", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var sqliteConn = (SqliteConnection) sqliteConnField.GetValue(conn)!;
        
        CreateLastUpdatedColumnIfNotExists(sqliteConn);
        ((SQLiteDBConnection)conn).OnOpened(); // Force an update of the setChunksCmd and setMapCHunksCmd queries
    }
    
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
                    _api.Logger.Notification($"[IncrementalWorldBackups] Found existing last_updated column on {tableName}");
                    // Already has last_updated column.
                    continue;
                }
            }
            
            _api.Logger.Notification($"[IncrementalWorldBackups] Adding last_updated column to {tableName}");
            using (var command = sqliteConnection.CreateCommand())
            {
                command.CommandText = $"ALTER TABLE {tableName} ADD COLUMN last_updated INTEGER NOT NULL DEFAULT 0;";
                Console.WriteLine(command.CommandText);
                command.ExecuteNonQuery();
            }

            _api.Logger.Notification($"[IncrementalWorldBackups] Adding last_updated index to {tableName}");
            using (var indexCmd = sqliteConnection.CreateCommand())
            {
                indexCmd.CommandText = $"CREATE INDEX IF NOT EXISTS index_{tableName}_last_updated ON {tableName}(last_updated);";
                indexCmd.ExecuteNonQuery();
            }
        }
    }
}