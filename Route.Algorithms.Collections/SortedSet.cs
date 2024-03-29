﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Route.Algorithms.Collections
{
    /// <summary>
    ///     Represents a strongly typed list of objects that will be sorted using the IComparable interface.
    /// </summary>
    /// <remarks>Re-implemented for compatibility with Windows Phone/Silverlight.</remarks>
    /// <typeparam name="T">The type of the elements in the list.</typeparam>
    public class SortedSet<T> : ISet<T>, ICollection<T>, IEnumerable<T>, ICollection, IEnumerable
    {
        /// <summary>
        ///     Holds the comparer for this set.
        /// </summary>
        private readonly IComparer<T> _comparer;

        /// <summary>
        ///     The list containing the elements.
        /// </summary>
        private readonly List<T> _elements;

        /// <summary>
        ///     Creates a new sorted set.
        /// </summary>
        public SortedSet()
        {
            _elements = new List<T>();
            _comparer = Comparer<T>.Default;
        }

        /// <summary>
        ///     Creates a new sorted set.
        /// </summary>
        public SortedSet(IEnumerable<T> enumerable)
        {
            _elements = new List<T>();
            _comparer = Comparer<T>.Default;

            foreach (var item in enumerable) Add(item);
        }

        /// <summary>
        ///     Creates a new sorted set.
        /// </summary>
        public SortedSet(IEnumerable<T> enumerable, IComparer<T> comparer)
        {
            _elements = new List<T>();
            _comparer = comparer;

            foreach (var item in enumerable) Add(item);
        }

        /// <summary>
        ///     Creates a new sorted set.
        /// </summary>
        public SortedSet(IComparer<T> comparer)
        {
            _elements = new List<T>();
            _comparer = comparer;
        }


        /// <summary>
        ///     Gets the System.Collections.Generic.IEqualityComparer object that is used
        ///     to determine equality for the values in the System.Collections.Generic.SortedSet.
        /// </summary>
        public IComparer<T> Comparer => _comparer;

        /// <summary>
        ///     Gets the maximum value in the System.Collections.Generic.SortedSet, as
        ///     defined by the comparer.
        /// </summary>
        public T Max => _elements[_elements.Count - 1];

        /// <summary>
        ///     Gets the minimum value in the System.Collections.Generic.SortedSet, as
        ///     defined by the comparer.
        /// </summary>
        public T Min => _elements[0];

        #region IEnumerable<T> Members

        /// <summary>
        ///     Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        ///     A System.Collections.Generic.IEnumerator that can be used to iterate through
        ///     the collection.
        /// </returns>
        public IEnumerator<T> GetEnumerator()
        {
            return _elements.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        /// <summary>
        ///     Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        ///     An System.Collections.IEnumerator object that can be used to iterate through
        ///     the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        #endregion

        /// <summary>
        ///     Explicit implementation of the ICollection<typeparamref name="T" />.Add method.
        /// </summary>
        /// <param name="item"></param>
        void ICollection<T>.Add(T item)
        {
            Add(item);
        }

        #region ICollection<T> Members

        /// <summary>
        ///     Adds an item to the <see cref="SortedSet{T}" />.
        /// </summary>
        /// <param name="item"></param>
        public bool Add(T item)
        {
            // intialize
            var idxLower = 0;
            var idxUpper = Count;
            var up = true;

            // loop
            int windowSize;
            int idxToTest;
            while (idxUpper - idxLower > 0)
            {
                // (re)calculate the windowsize.
                windowSize = idxUpper - idxLower;
                // divide by two
                // and round the value (Floor function).
                idxToTest = windowSize / 2 + idxLower;

                // test the element at the given index.
                up = _comparer.Compare(_elements[idxToTest], item) < 0;

                // update the index
                if (up)
                    idxLower = idxToTest + 1;
                else
                    idxUpper = idxToTest;
            }

            // insert
            _elements.Insert(idxLower, item);
            return false;
        }

        /// <summary>
        ///     Removes all items from the <see cref="SortedSet{T}" />.
        /// </summary>
        public void Clear()
        {
            _elements.Clear();
        }

        /// <summary>
        ///     Determines whether the <see cref="SortedSet{T}" /> contains
        ///     a specific value.
        /// </summary>
        /// <param name="item">The object to locate in the <see cref="SortedSet{T}" />.</param>
        /// <returns>
        ///     true if item is found in the <see cref="SortedSet{T}" />; otherwise,
        ///     false.
        /// </returns>
        public bool Contains(T item)
        {
            return _elements.Contains(item);
        }

        /// <summary>
        ///     Copies the elements of the <see cref="SortedSet{T}" /> to an
        ///     System.Array, starting at a particular System.Array index.
        /// </summary>
        /// <param name="array">
        ///     The one-dimensional System.Array that is the destination of the elements
        ///     copied from <see cref="SortedSet{T}" />. The System.Array must
        ///     have zero-based indexing.
        /// </param>
        /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            foreach (var item in this)
            {
                array[arrayIndex] = item;
                arrayIndex++;
            }
        }

        /// <summary>
        ///     Gets the number of elements contained in the <see cref="SortedSet{T}" />.
        /// </summary>
        public int Count => _elements.Count;

        /// <summary>
        ///     Gets a value indicating whether the SortedSet is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        ///     Removes the first occurrence of a specific object from the <see cref="SortedSet{T}" />.
        /// </summary>
        /// <param name="item">The object to remove from the <see cref="SortedSet{T}" />.</param>
        /// <returns>
        ///     true if item was successfully removed from the <see cref="SortedSet{T}" />;
        ///     otherwise, false. This method also returns false if item is not found in
        ///     the original <see cref="SortedSet{T}" />.
        /// </returns>
        public bool Remove(T item)
        {
            return _elements.Remove(item);
        }

        #endregion

        #region ICollection Members

        /// <summary>
        ///     Copies all objects to the given array starting at the given location.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="index"></param>
        public void CopyTo(Array array, int index)
        {
            foreach (var item in this)
            {
                array.SetValue(item, index);
                index++;
            }
        }

        /// <summary>
        ///     Gets a value indicating whether access to the System.Collections.ICollection is synchronized (thread safe).
        /// </summary>
        public bool IsSynchronized => false;

        /// <summary>
        ///     Gets an object that can be used to synchronize access to the System.Collections.ICollection.
        /// </summary>
        public object SyncRoot => this;

        #endregion

        #region ISet<T> Members

        /// <summary>
        ///     Removes all elements in the specified collection.
        /// </summary>
        /// <param name="other"></param>
        public void ExceptWith(IEnumerable<T> other)
        {
            foreach (var item in other) Remove(item);
        }

        /// <summary>
        ///     Keeps only the elements that are in the given collection and in this set.
        /// </summary>
        /// <param name="other"></param>
        public void IntersectWith(IEnumerable<T> other)
        {
            // calculate intersection.
            var intersection = new HashSet<T>();
            foreach (var item in other)
                if (Contains(item))
                    intersection.Add(item);

            // clear this set.
            Clear();
            UnionWith(intersection);
        }

        /// <summary>
        ///     Returns true if all elements in the given collection are contained in this set.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            // TODO: very naive implementation for now!
            var other_set = new HashSet<T>(other);
            foreach (var item in this)
                if (!other_set.Contains(item))
                    return false;
            // make sure this is proper.
            foreach (var item in other)
                if (!Contains(item))
                    return true;
            return false;
        }

        /// <summary>
        ///     Returns true if all elements in this set are also in the given collection.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            foreach (var item in other)
                if (!Contains(item))
                    return false;
            // make sure this is proper.
            // TODO: very naive implementation for now!
            var other_set = new HashSet<T>(other);
            foreach (var item in this)
                if (!other_set.Contains(item))
                    return true;
            return false;
        }

        /// <summary>
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsSubsetOf(IEnumerable<T> other)
        {
            // TODO: very naive implementation for now!
            var other_set = new HashSet<T>(other);
            foreach (var item in this)
                if (!other_set.Contains(item))
                    return false;
            return true;
        }

        /// <summary>
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            foreach (var item in other)
                if (!Contains(item))
                    return false;
            return true;
        }

        /// <summary>
        ///     Returns true if this collection and the other share at least one element.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Overlaps(IEnumerable<T> other)
        {
            foreach (var item in other)
                if (Contains(item))
                    return true;
            // TODO: very naive implementation for now!
            var other_set = new HashSet<T>(other);
            foreach (var item in this)
                if (other_set.Contains(item))
                    return true;
            return false;
        }

        /// <summary>
        ///     Returns true if the two sets contain the same elements.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool SetEquals(IEnumerable<T> other)
        {
            var other_set = new HashSet<T>(other);
            foreach (var item in this) other_set.Remove(item);
            return other_set.Count == 0;
        }

        /// <summary>
        ///     Keeps only items that are in one set or the other but not in both.
        /// </summary>
        /// <param name="other"></param>
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            foreach (var item in this.Intersect(other)) Remove(item);
        }

        /// <summary>
        ///     Adds all items in other too.
        /// </summary>
        /// <param name="other"></param>
        public void UnionWith(IEnumerable<T> other)
        {
            foreach (var item in other)
                if (!Contains(item))
                    Add(item);
        }

        #endregion
    }
}