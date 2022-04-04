﻿using System;
using System.Collections.Generic;
using System.IO;

namespace Route.IO.Json
{
    /// <summary>
    ///     A json-writer.
    /// </summary>
    public class JsonWriter
    {
        private readonly Stack<Status> _statusStack;

        /// <summary>
        ///     Creates a new json writer.
        /// </summary>
        public JsonWriter(TextWriter writer)
        {
            TextWriter = writer;
            _statusStack = new Stack<Status>();
        }

        /// <summary>
        ///     Gets the text writer.
        /// </summary>
        /// <returns></returns>
        public TextWriter TextWriter { get; }

        /// <summary>
        ///     Writes the object open char.
        /// </summary>
        public void WriteOpen()
        {
            if (_statusStack.Count > 0)
            {
                var status = _statusStack.Peek();

                if (status == Status.ArrayValueWritten) TextWriter.Write(',');
            }

            _statusStack.Push(Status.ObjectOpened);
            TextWriter.Write('{');
        }

        /// <summary>
        ///     Writes the object close char.
        /// </summary>
        public void WriteClose()
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot close object at this point.");
            if (_statusStack.Count > 0)
            {
                status = _statusStack.Peek();

                if (status == Status.PropertyNameWritten)
                    throw new Exception("Cannot close object right after writing a property name.");
            }

            TextWriter.Write('}');
            while (_statusStack.Peek() != Status.ObjectOpened) _statusStack.Pop();
            _statusStack.Pop();

            if (_statusStack.Count > 0)
            {
                status = _statusStack.Peek();
                if (status == Status.PropertyNameWritten)
                    // the object was a property value.
                    _statusStack.Push(Status.PropertyValueWritten);
                if (status == Status.ArrayOpenWritten ||
                    status == Status.ArrayValueWritten)
                    // the array was an array value.
                    _statusStack.Push(Status.ArrayValueWritten);
            }
        }

        /// <summary>
        ///     Writes a property name.
        /// </summary>
        public void WritePropertyName(string name, bool escape = false)
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot write property name at this point.");

            status = _statusStack.Peek();
            if (status != Status.PropertyValueWritten &&
                status != Status.ObjectOpened)
                throw new Exception("Cannot write property name at this point.");

            if (status == Status.PropertyValueWritten)
                // write comma before starting new property.
                TextWriter.Write(',');

            TextWriter.Write('"');
            if (escape) name = JsonTools.Escape(name);
            TextWriter.Write(name);
            TextWriter.Write('"');
            TextWriter.Write(':');
            _statusStack.Push(Status.PropertyNameWritten);
        }

        /// <summary>
        ///     Writes a property value.
        /// </summary>
        public void WritePropertyValue(string value, bool useQuotes = false, bool escape = false)
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot write property value at this point.");

            status = _statusStack.Peek();
            if (status != Status.PropertyNameWritten) throw new Exception("Cannot write property value at this point.");

            if (useQuotes) TextWriter.Write('"');
            if (escape) value = JsonTools.Escape(value);
            TextWriter.Write(value);
            if (useQuotes) TextWriter.Write('"');
            _statusStack.Push(Status.PropertyValueWritten);
        }

        /// <summary>
        ///     Writes a property and it's value.
        /// </summary>
        public void WriteProperty(string name, string value, bool useQuotes = false, bool escape = false)
        {
            WritePropertyName(name, escape);
            WritePropertyValue(value, useQuotes, escape);
        }

        /// <summary>
        ///     Writes the array open char.
        /// </summary>
        public void WriteArrayOpen()
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot open array at this point.");

            status = _statusStack.Peek();
            if (status != Status.PropertyNameWritten &&
                status != Status.ArrayOpenWritten &&
                status != Status.ArrayValueWritten)
                throw new Exception("Cannot open array at this point.");

            if (status == Status.ArrayValueWritten) TextWriter.Write(',');

            TextWriter.Write('[');
            _statusStack.Push(Status.ArrayOpenWritten);
        }

        /// <summary>
        ///     Writes the array close char.
        /// </summary>
        public void WriteArrayClose()
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot open array at this point.");

            status = _statusStack.Peek();
            if (status != Status.ArrayOpenWritten &&
                status != Status.ArrayValueWritten)
                throw new Exception("Cannot open array at this point.");

            TextWriter.Write(']');

            status = _statusStack.Peek();
            while (status != Status.ArrayOpenWritten)
            {
                _statusStack.Pop();
                status = _statusStack.Peek();
            }

            _statusStack.Pop();


            if (_statusStack.Count > 0)
            {
                status = _statusStack.Peek();
                if (status == Status.PropertyNameWritten)
                    // the array was a property value.
                    _statusStack.Push(Status.PropertyValueWritten);
                if (status == Status.ArrayOpenWritten ||
                    status == Status.ArrayValueWritten)
                    // the array was an array value.
                    _statusStack.Push(Status.ArrayValueWritten);
            }
        }

        /// <summary>
        ///     Writes an array value.
        /// </summary>
        public void WriteArrayValue(string value)
        {
            Status status;
            if (_statusStack.Count == 0) throw new Exception("Cannot open array at this point.");

            status = _statusStack.Peek();
            if (status != Status.ArrayOpenWritten &&
                status != Status.ArrayValueWritten)
                throw new Exception("Cannot open array at this point.");

            if (status == Status.ArrayValueWritten) TextWriter.Write(",");

            TextWriter.Write(value);
            _statusStack.Push(Status.ArrayValueWritten);
        }

        private enum Status
        {
            ObjectOpened,
            PropertyNameWritten,
            PropertyValueWritten,
            ArrayOpenWritten,
            ArrayValueWritten
        }
    }
}