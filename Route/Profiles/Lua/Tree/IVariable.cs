
namespace Route.Profiles.Lua.Tree
{
	interface IVariable
	{
		void CompileAssignment(Execution.VM.ByteCode bc, int stackofs, int tupleidx);
	}
}
