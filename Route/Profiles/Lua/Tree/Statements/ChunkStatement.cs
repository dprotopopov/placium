using Route.Profiles.Lua.DataTypes;
using Route.Profiles.Lua.Errors;
using Route.Profiles.Lua.Execution;
using Route.Profiles.Lua.Execution.Scopes;
using Route.Profiles.Lua.Execution.VM;
using Route.Profiles.Lua.Tree.Lexer;

namespace Route.Profiles.Lua.Tree.Statements
{
	class ChunkStatement : Statement, IClosureBuilder
	{
		Statement m_Block;
		RuntimeScopeFrame m_StackFrame;
		SymbolRef m_Env;
		SymbolRef m_VarArgs;

		public ChunkStatement(ScriptLoadingContext lcontext)
			: base(lcontext)
		{
			lcontext.Scope.PushFunction(this, true);
			m_Env = lcontext.Scope.DefineLocal(WellKnownSymbols.ENV);
			m_VarArgs = lcontext.Scope.DefineLocal(WellKnownSymbols.VARARGS);

			m_Block = new CompositeStatement(lcontext);

			if (lcontext.Lexer.Current.Type != TokenType.Eof)
				throw new SyntaxErrorException(lcontext.Lexer.Current, "<eof> expected near '{0}'", lcontext.Lexer.Current.Text);

			m_StackFrame = lcontext.Scope.PopFunction();
		}


		public override void Compile(Execution.VM.ByteCode bc)
		{
			Instruction meta = bc.Emit_Meta("<chunk-root>", OpCodeMetadataType.ChunkEntrypoint);
			int metaip = bc.GetJumpPointForLastInstruction();

			bc.Emit_BeginFn(m_StackFrame);
			bc.Emit_Args(m_VarArgs);

			bc.Emit_Load(SymbolRef.Upvalue(WellKnownSymbols.ENV, 0));
			bc.Emit_Store(m_Env, 0, 0);
			bc.Emit_Pop();

			m_Block.Compile(bc);
			bc.Emit_Ret(0);

			meta.NumVal = bc.GetJumpPointForLastInstruction() - metaip;
		}

		public SymbolRef CreateUpvalue(BuildTimeScope scope, SymbolRef symbol)
		{
			return null;
		}
	}
}
