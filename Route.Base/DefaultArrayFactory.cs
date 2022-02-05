﻿using Reminiscence.Arrays;

namespace Route;

/// <summary>
///     Implementation of <see cref="IArrayFactory" /> which uses the default
///     array types implemented and exposed in Reminiscence.
/// </summary>
public sealed class DefaultArrayFactory : IArrayFactory
{
    /// <inheritdoc />
    public ArrayBase<T> CreateMemoryBackedArray<T>(long size)
    {
        return new MemoryArray<T>(size);
    }
}