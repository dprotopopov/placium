using System;
using Route.Profiles.Lua.Interop.BasicDescriptors;

namespace Route.Profiles.Lua.Interop.StandardDescriptors.HardwiredDescriptors
{
	public abstract class HardwiredUserDataDescriptor : DispatchingUserDataDescriptor
	{
		protected HardwiredUserDataDescriptor(Type T) :
			base(T, "::hardwired::" + T.Name)
		{

		}

	}
}
