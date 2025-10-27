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

public class WorldSaveProofOfConceptModSystem : ModSystem
{
    private ICoreServerAPI _api = null!;
    
    public override bool ShouldLoad(EnumAppSide forSide) => forSide == EnumAppSide.Server;

    public override void StartServerSide(ICoreServerAPI api)
    {
        _api = api;
        SetGameDatabaseConnection();
    }

    private void SetGameDatabaseConnection()
    {
        var serverMain = (ServerMain) typeof(ServerProgram).GetField("server")!.GetValue(null)!;
        var chunkThread = (ChunkServerThread) serverMain.GetType().GetField("chunkThread")!.GetValue(serverMain)!;
        var database = (GameDatabase) chunkThread.GetType().GetField("gameDatabase")!.GetValue(chunkThread)!;
        var connField = database.GetType().GetField("conn")!;
        var existingValue = (IGameDbConnection) connField.GetValue(database)!;

        if (existingValue is IncrementalBackupsGameDbConnection)
        {
            _api.Logger.Debug("Already using our custom database connection");
            return;
        }
        
        _api.Logger.Debug("Swapping in our game database connection");
        var connection = new IncrementalBackupsGameDbConnection(_api.Logger);
        connection.CreateLastUpdatedColumnIfNotExists();
        connField.SetValue(database, connection);
    }
}