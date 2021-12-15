﻿using Route.Profiles.Lua.DataTypes;
using Route.Profiles.Lua.Debugging;
using Route.Profiles.Lua.Execution.Scopes;

namespace Route.Profiles.Lua.Execution.VM
{
	internal class CallStackItem
	{
		public int Debug_EntryPoint;
		public SymbolRef[] Debug_Symbols;

		public SourceRef CallingSourceRef;

		public CallbackFunction ClrFunction;
		public CallbackFunction Continuation;
		public CallbackFunction ErrorHandler;
		public DynValue ErrorHandlerBeforeUnwind;

		public int BasePointer;
		public int ReturnAddress;
		public DynValue[] LocalScope;
		public ClosureContext ClosureScope;

		public CallStackItemFlags Flags;
	}

}
