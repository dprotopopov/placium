using Route.Profiles.Lua.Debugging;
using Route.Profiles.Lua.Execution.Scopes;
using Route.Profiles.Lua.Tree.Lexer;

namespace Route.Profiles.Lua.Execution
{
	class ScriptLoadingContext
	{
		public Script Script { get; private set; }
		public BuildTimeScope Scope { get; set; }
		public SourceCode Source { get; set; }
		public bool Anonymous { get; set; }
		public bool IsDynamicExpression { get; set; }
		public Lexer Lexer { get; set; }

		public ScriptLoadingContext(Script s)
		{
			Script = s;
		}

	}
}
