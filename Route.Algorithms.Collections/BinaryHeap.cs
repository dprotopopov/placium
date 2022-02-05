/*
 *  Licensed to SharpSoftware under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for 
 *  additional information regarding copyright ownership.
 * 
 *  SharpSoftware licenses this file to you under the Apache License, 
 *  Version 2.0 (the "License"); you may not use this file except in 
 *  compliance with the License. You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

using System;

namespace Route.Algorithms.Collections;

/// <summary>
///     Implements a priority queue in the form of a binairy heap.
/// </summary>
public class BinaryHeap<T>
{
    private T[] _heap; // The objects per priority.
    private long _latestIndex; // The latest unused index
    private float[] _priorities; // Holds the priorities of this heap.

    /// <summary>
    ///     Creates a new binairy heap.
    /// </summary>
    public BinaryHeap()
        : this(2)
    {
    }

    /// <summary>
    ///     Creates a new binairy heap.
    /// </summary>
    public BinaryHeap(long initialSize)
    {
        _heap = new T[initialSize];
        _priorities = new float[initialSize];

        Count = 0;
        _latestIndex = 1;
    }

    /// <summary>
    ///     Returns the number of items in this queue.
    /// </summary>
    public int Count { get; private set; }

    /// <summary>
    ///     Enqueues a given item.
    /// </summary>
    public void Push(T item, float priority)
    {
        Count++; // another item was added!

        // increase size if needed.
        if (_latestIndex == _priorities.Length - 1)
        {
            // time to increase size!
            Array.Resize(ref _heap, _heap.Length + 100);
            Array.Resize(ref _priorities, _priorities.Length + 100);
        }

        // add the item at the first free point 
        _priorities[_latestIndex] = priority;
        _heap[_latestIndex] = item;

        // ... and let it 'bubble' up.
        var bubbleIndex = _latestIndex;
        _latestIndex++;
        while (bubbleIndex != 1)
        {
            // bubble until the indx is one.
            var parentIdx = bubbleIndex / 2;
            if (_priorities[bubbleIndex] < _priorities[parentIdx])
            {
                // the parent priority is higher; do the swap.
                var tempPriority = _priorities[parentIdx];
                var tempItem = _heap[parentIdx];
                _priorities[parentIdx] = _priorities[bubbleIndex];
                _heap[parentIdx] = _heap[bubbleIndex];
                _priorities[bubbleIndex] = tempPriority;
                _heap[bubbleIndex] = tempItem;

                bubbleIndex = parentIdx;
            }
            else
            {
                // the parent priority is lower or equal; the item will not bubble up more.
                break;
            }
        }
    }

    /// <summary>
    ///     Returns the smallest weight in the queue.
    /// </summary>
    public float PeekWeight()
    {
        return _priorities[1];
    }

    /// <summary>
    ///     Returns the object with the smallest weight.
    /// </summary>
    public T Peek()
    {
        return _heap[1];
    }

    /// <summary>
    ///     Returns the object with the smallest weight and removes it.
    /// </summary>
    public T Pop()
    {
        if (Count > 0)
        {
            var item = _heap[1]; // get the first item.

            Count--; // reduce the element count.
            _latestIndex--; // reduce the latest index.

            int swapitem = 1, parent = 1;
            float swapItemPriority = 0;
            var parentPriority = _priorities[_latestIndex];
            var parentItem = _heap[_latestIndex];
            _heap[1] = parentItem; // place the last element on top.
            _priorities[1] = parentPriority; // place the last element on top.
            do
            {
                parent = swapitem;
                if (2 * parent + 1 <= _latestIndex)
                {
                    swapItemPriority = _priorities[2 * parent];
                    var potentialSwapItem = _priorities[2 * parent + 1];
                    if (parentPriority >= swapItemPriority)
                    {
                        swapitem = 2 * parent;
                        if (_priorities[swapitem] >= potentialSwapItem)
                        {
                            swapItemPriority = potentialSwapItem;
                            swapitem = 2 * parent + 1;
                        }
                    }
                    else if (parentPriority >= potentialSwapItem)
                    {
                        swapItemPriority = potentialSwapItem;
                        swapitem = 2 * parent + 1;
                    }
                    else
                    {
                        break;
                    }
                }
                else if (2 * parent <= _latestIndex)
                {
                    // Only one child exists
                    swapItemPriority = _priorities[2 * parent];
                    if (parentPriority >= swapItemPriority)
                        swapitem = 2 * parent;
                    else
                        break;
                }
                else
                {
                    break;
                }

                _priorities[parent] = swapItemPriority;
                _priorities[swapitem] = parentPriority;
                _heap[parent] = _heap[swapitem];
                _heap[swapitem] = parentItem;
            } while (true);

            return item;
        }

        return default;
    }

    /// <summary>
    ///     Clears this priority queue.
    /// </summary>
    public void Clear()
    {
        Count = 0;
        _latestIndex = 1;
    }
}