﻿
using System;
using Reminiscence.Arrays;

namespace Route.Algorithms.Collections
{
    /// <summary>
    ///     A stack implementation based on a memory array.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Stack<T>
    {
        private readonly ArrayBase<T> _data;

        private int _pointer = -1;

        /// <summary>
        ///     Creates a new stack.
        /// </summary>
        public Stack()
        {
            _data = Context.ArrayFactory.CreateMemoryBackedArray<T>(1024);
        }

        /// <summary>
        ///     Gets the number of elements on this stack.
        /// </summary>
        public int Count => _pointer + 1;

        /// <summary>
        ///     Pushes a new element.
        /// </summary>
        public void Push(T element)
        {
            if (_pointer >= _data.Length - 1) _data.Resize(_data.Length + 1024);

            _pointer++;
            _data[_pointer] = element;
        }

        /// <summary>
        ///     Returns the element at the top.
        /// </summary>
        /// <returns></returns>
        public T Peek()
        {
            if (_pointer == -1) throw new InvalidOperationException("Cannot peek into an empty stack.");
            return _data[_pointer];
        }

        /// <summary>
        ///     Pops an element from the stack.
        /// </summary>
        /// <returns></returns>
        public T Pop()
        {
            if (_pointer == -1) throw new InvalidOperationException("Cannot pop from an empty stack.");
            var e = _data[_pointer];
            _pointer--;
            return e;
        }
    }
}