﻿using Route.Profiles.Lua.DataStructs;
using Route.Profiles.Lua.DataTypes;

namespace Route.Profiles.Lua.Execution.VM
{
	internal sealed class ExecutionState
	{
		public FastStack<DynValue> ValueStack = new FastStack<DynValue>(131072);
		public FastStack<CallStackItem> ExecutionStack = new FastStack<CallStackItem>(131072);
		public int InstructionPtr = 0;
		public CoroutineState State = CoroutineState.NotStarted;
	}
}
