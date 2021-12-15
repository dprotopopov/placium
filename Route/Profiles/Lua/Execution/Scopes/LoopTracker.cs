using Route.Profiles.Lua.DataStructs;
using Route.Profiles.Lua.Execution.VM;

namespace Route.Profiles.Lua.Execution.Scopes
{
	interface ILoop
	{
		void CompileBreak(ByteCode bc);
		bool IsBoundary();
	}


	internal class LoopTracker
	{
		public FastStack<ILoop> Loops = new FastStack<ILoop>(16384);
	}
}
