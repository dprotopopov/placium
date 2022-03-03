using System;
using System.Globalization;
using System.IO;
using System.Text;
using Reminiscence.Arrays;

namespace Route.Attributes
{
    public static class Extensions
    {
        /// <summary>
        ///     Reads a string.
        /// </summary>
        public static string ReadWithSizeString(this Stream stream)
        {
            var longBytes = new byte[8];
            stream.Read(longBytes, 0, 8);
            var size = BitConverter.ToInt64(longBytes, 0);
            var data = new byte[size];
            stream.Read(data, 0, (int)size);

            return Encoding.Unicode.GetString(data, 0, data.Length);
        }

        /// <summary>
        ///     Writes the given value with size prefix.
        /// </summary>
        public static long WriteWithSize(this Stream stream, string value)
        {
            var bytes = Encoding.Unicode.GetBytes(value);
            return stream.WriteWithSize(bytes);
        }

        /// <summary>
        ///     Writes the given value with size prefix.
        /// </summary>
        public static long WriteWithSize(this Stream stream, byte[] value)
        {
            stream.Write(BitConverter.GetBytes((long)value.Length), 0, 8);
            stream.Write(value, 0, value.Length);
            return value.Length + 8;
        }


        /// <summary>
        ///     Returns a string representing the object in a culture invariant way.
        /// </summary>
        public static string ToInvariantString(this object obj)
        {
            return obj is IConvertible ? ((IConvertible)obj).ToString(CultureInfo.InvariantCulture)
                : obj is IFormattable ? ((IFormattable)obj).ToString(null, CultureInfo.InvariantCulture)
                : obj.ToString();
        }

        /// <summary>
        ///     Ensures that this <see cref="ArrayBase{T}" /> has room for at least
        ///     the given number of elements, resizing if not.
        /// </summary>
        /// <typeparam name="T">
        ///     The type of element stored in this array.
        /// </typeparam>
        /// <param name="array">
        ///     This array.
        /// </param>
        /// <param name="minimumSize">
        ///     The minimum number of elements that this array must fit.
        /// </param>
        public static void EnsureMinimumSize<T>(this ArrayBase<T> array, long minimumSize)
        {
            if (array.Length < minimumSize) IncreaseMinimumSize(array, minimumSize, false, default);
        }

        /// <summary>
        ///     Ensures that this <see cref="ArrayBase{T}" /> has room for at least
        ///     the given number of elements, resizing and filling the empty space
        ///     with the given value if not.
        /// </summary>
        /// <typeparam name="T">
        ///     The type of element stored in this array.
        /// </typeparam>
        /// <param name="array">
        ///     This array.
        /// </param>
        /// <param name="minimumSize">
        ///     The minimum number of elements that this array must fit.
        /// </param>
        /// <param name="fillValue">
        ///     The value to use to fill in the empty spaces if we have to resize.
        /// </param>
        public static void EnsureMinimumSize<T>(this ArrayBase<T> array, long minimumSize, T fillValue)
        {
            if (array.Length < minimumSize) IncreaseMinimumSize(array, minimumSize, true, fillValue);
        }

        private static void IncreaseMinimumSize<T>(ArrayBase<T> array, long minimumSize, bool fillEnd,
            T fillValueIfNeeded)
        {
            var oldSize = array.Length;

            // fast-forward, perhaps, through the first several resizes.
            // Math.Max also ensures that we can resize from 0.
            var size = Math.Max(1024, oldSize * 2);
            while (size < minimumSize) size *= 2;

            array.Resize(size);
            if (!fillEnd) return;

            for (var i = oldSize; i < size; i++) array[i] = fillValueIfNeeded;
        }
    }
}