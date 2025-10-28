using System.Collections.Generic;
using System.Reflection.Emit;
using HarmonyLib;
using Vintagestory.Common.Database;

namespace IncrementalWorldBackups.Harmony.Patches;
    
[HarmonyPatch]
public static class SQLiteDbConnectionv2Patches
{
    [HarmonyTranspiler]
    [HarmonyPatch(typeof(SQLiteDbConnectionv2), "OnOpened")]
    private static IEnumerable<CodeInstruction> OnOpened_ReplaceQueryStrings(IEnumerable<CodeInstruction> instructions)
    {
        foreach (var instr in instructions)
        {
            if (instr.opcode == OpCodes.Ldstr)
            {
                switch ((string) instr.operand)
                {
                    case "INSERT OR REPLACE INTO chunk (position, data) VALUES (@position,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT OR REPLACE INTO chunk (position, data, last_updated) VALUES (@position,@data,unixepoch('now'))");
                        continue;
                    case "INSERT OR REPLACE INTO mapchunk (position, data) VALUES (@position,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT OR REPLACE INTO mapchunk (position, data, last_updated) VALUES (@position,@data,unixepoch('now'))");
                        continue;
                }
            }
            yield return instr;
        }
    }
    
    [HarmonyTranspiler]
    [HarmonyPatch(typeof(SQLiteDbConnectionv2), "SetPlayerData")]
    private static IEnumerable<CodeInstruction> SetPlayerData_ReplaceQueryStrings(IEnumerable<CodeInstruction> instructions)
    {
        foreach (var instr in instructions)
        {
            if (instr.opcode == OpCodes.Ldstr)
            {
                switch ((string) instr.operand)
                {
                    case "INSERT INTO playerdata (playeruid, data) VALUES (@playeruid,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT INTO playerdata (playeruid, data, last_updated) VALUES (@playeruid,@data,unixepoch('now'))");
                        continue;
                    case "UPDATE playerdata set data=@data where playeruid=@playeruid":
                        yield return new CodeInstruction(instr.opcode, "UPDATE playerdata set data=@data,last_updated=unixepoch('now') where playeruid=@playeruid");
                        continue;
                }
            }
            yield return instr;
        }
    }
    
    [HarmonyTranspiler]
    [HarmonyPatch(typeof(SQLiteDbConnectionv2), "InsertChunk")]
    private static IEnumerable<CodeInstruction> InsertChunk_ReplaceQueryStrings(IEnumerable<CodeInstruction> instructions)
    {
        foreach (var instr in instructions)
        {
            if (instr.opcode == OpCodes.Ldstr)
            {
                switch ((string) instr.operand)
                {
                    case "INSERT OR REPLACE INTO mapregion (position, data) VALUES (@position,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT OR REPLACE INTO mapregion (position, data, last_updated) VALUES (@position,@data,unixepoch('now'))");
                        continue;
                    case "INSERT OR REPLACE INTO mapchunk (position, data) VALUES (@position,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT OR REPLACE INTO mapchunk (position, data, last_updated) VALUES (@position,@data,unixepoch('now'))");
                        continue;
                    case "INSERT OR REPLACE INTO chunk (position, data) VALUES (@position,@data)":
                        yield return new CodeInstruction(instr.opcode, "INSERT OR REPLACE INTO chunk (position, data, last_updated) VALUES (@position,@data,unixepoch('now'))");
                        continue;
                }
            }
            yield return instr;
        }
    }
    
    [HarmonyTranspiler]
    [HarmonyPatch(typeof(SQLiteDbConnectionv2), "CreateTablesIfNotExists")]
    private static IEnumerable<CodeInstruction> CreateTablesIfNotExists_ReplaceQueryStrings(IEnumerable<CodeInstruction> instructions)
    {
        foreach (var instr in instructions)
        {
            if (instr.opcode == OpCodes.Ldstr)
            {
                switch ((string) instr.operand)
                {
                    case "CREATE TABLE IF NOT EXISTS chunk (position integer PRIMARY KEY, data BLOB);":
                        yield return new CodeInstruction(instr.opcode, "CREATE TABLE IF NOT EXISTS chunk (position integer PRIMARY KEY, data BLOB, last_updated INTEGER NOT NULL DEFAULT 0);");
                        continue;
                    case "CREATE TABLE IF NOT EXISTS mapchunk (position integer PRIMARY KEY, data BLOB);":
                        yield return new CodeInstruction(instr.opcode, "CREATE TABLE IF NOT EXISTS mapchunk (position integer PRIMARY KEY, data BLOB, last_updated INTEGER NOT NULL DEFAULT 0);");
                        continue;
                    case "CREATE TABLE IF NOT EXISTS mapregion (position integer PRIMARY KEY, data BLOB);":
                        yield return new CodeInstruction(instr.opcode, "CREATE TABLE IF NOT EXISTS mapregion (position integer PRIMARY KEY, data BLOB, last_updated INTEGER NOT NULL DEFAULT 0);");
                        continue;
                    case "CREATE TABLE IF NOT EXISTS gamedata (savegameid integer PRIMARY KEY, data BLOB);":
                        yield return new CodeInstruction(instr.opcode, "CREATE TABLE IF NOT EXISTS gamedata (savegameid integer PRIMARY KEY, data BLOB, last_updated INTEGER NOT NULL DEFAULT 0);");
                        continue;
                    case "CREATE TABLE IF NOT EXISTS playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB);":
                        yield return new CodeInstruction(instr.opcode, "CREATE TABLE IF NOT EXISTS playerdata (playerid integer PRIMARY KEY AUTOINCREMENT, playeruid TEXT, data BLOB, last_updated INTEGER NOT NULL DEFAULT 0);");
                        continue;
                    case "CREATE index IF NOT EXISTS index_playeruid on playerdata(playeruid);":
                        yield return new CodeInstruction(instr.opcode, "CREATE index IF NOT EXISTS index_playeruid on playerdata(playeruid);");
                        continue;

                }
            }
            yield return instr;
        }
    }
}