using Route.Profiles.Lua.Compatibility.Frameworks;
using Route.Profiles.Lua.Compatibility.Frameworks.Base;

namespace Route.Profiles.Lua.Compatibility
{
	public static class Framework
	{
		static FrameworkCurrent s_FrameworkCurrent = new FrameworkCurrent();

		public static FrameworkBase Do { get { return s_FrameworkCurrent; } }
	}
}
