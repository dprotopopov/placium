﻿using System;
using Route.Profiles.Lua.DataTypes;
using Route.Profiles.Lua.Execution;

namespace Route.Profiles.Lua.Tree.Expressions
{
	class DynamicExprExpression : Expression
	{
		Expression m_Exp;

		public DynamicExprExpression(Expression exp, ScriptLoadingContext lcontext)
			: base(lcontext)
		{
			lcontext.Anonymous = true;
			m_Exp = exp;
		}


		public override DynValue Eval(ScriptExecutionContext context)
		{
			return m_Exp.Eval(context);
		}

		public override void Compile(Execution.VM.ByteCode bc)
		{
			throw new InvalidOperationException();
		}

		public override SymbolRef FindDynamic(ScriptExecutionContext context)
		{
			return m_Exp.FindDynamic(context);
		}
	}
}
