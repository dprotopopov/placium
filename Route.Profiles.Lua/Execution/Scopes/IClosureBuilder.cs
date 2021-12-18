
using Route.Profiles.Lua.DataTypes;

namespace Route.Profiles.Lua.Execution.Scopes
{
	internal interface IClosureBuilder
	{
		SymbolRef CreateUpvalue(BuildTimeScope scope, SymbolRef symbol);

	}
}
