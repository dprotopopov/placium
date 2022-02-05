using System;
using System.Collections.Generic;

namespace Route.Attributes;

/// <summary>
///     An implementation of the EqualityComparer that allows the use of delegates.
/// </summary>
/// <typeparam name="T"></typeparam>
public class DelegateEqualityComparer<T> : IEqualityComparer<T>
{
    /// <summary>
    ///     A delegate to compare two objects.
    /// </summary>
    public delegate bool EqualsDelegate(T x, T y);

    /// <summary>
    ///     A delegate to calculate the hashcode.
    /// </summary>
    public delegate int GetHashCodeDelegate(T obj);

    /// <summary>
    ///     Holds the equals delegate.
    /// </summary>
    private readonly EqualsDelegate _equalsDelegate;

    /// <summary>
    ///     Holds the hashcode delegate.
    /// </summary>
    private readonly GetHashCodeDelegate _hashCodeDelegate;

    /// <summary>
    ///     Creates a new equality comparer.
    /// </summary>
    public DelegateEqualityComparer(GetHashCodeDelegate hashCodeDelegate, EqualsDelegate equalsDelegate)
    {
        if (hashCodeDelegate == null) throw new ArgumentNullException("hashCodeDelegate");
        if (equalsDelegate == null) throw new ArgumentNullException("equalsDelegate");

        _equalsDelegate = equalsDelegate;
        _hashCodeDelegate = hashCodeDelegate;
    }

    /// <summary>
    ///     Returns true if the two given objects are considered equal.
    /// </summary>
    public bool Equals(T x, T y)
    {
        return _equalsDelegate.Invoke(x, y);
    }

    /// <summary>
    ///     Calculates the hashcode for the given object.
    /// </summary>
    public int GetHashCode(T obj)
    {
        return _hashCodeDelegate.Invoke(obj);
    }
}