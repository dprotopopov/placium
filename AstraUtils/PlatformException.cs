using System;
using System.Runtime.InteropServices;
using Tmds.Linux;

namespace AstraUtils;

internal class PlatformException : Exception
{
    public PlatformException(int errno) :
        base(GetErrorMessage(errno))
    {
        HResult = errno;
    }

    public PlatformException() :
        this(LibC.errno)
    {
    }

    private static unsafe string GetErrorMessage(int errno)
    {
        var bufferLength = 1024;
        var buffer = stackalloc byte[bufferLength];

        var rv = LibC.strerror_r(errno, buffer, bufferLength);


        return rv == 0 ? Marshal.PtrToStringAnsi((IntPtr)buffer) : $"errno {errno}";
    }

    public static void Throw()
    {
        throw new PlatformException();
    }
}