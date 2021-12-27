using System;
using System.Collections.Generic;
using System.Globalization;
using OsmSharp.Tags;
using Route.Attributes;

namespace Placium.Route
{
    public static class Extensions
    {
        /// <summary>
        ///     Returns a string representing the object in a culture invariant way.
        /// </summary>
        public static string ToInvariantString(this object obj)
        {
            return obj is IConvertible ? ((IConvertible) obj).ToString(CultureInfo.InvariantCulture)
                : obj is IFormattable ? ((IFormattable) obj).ToString(null, CultureInfo.InvariantCulture)
                : obj.ToString();
        }

        /// <summary>
        ///     Converts the given tags collection to an attributes collection.
        /// </summary>
        public static IAttributeCollection ToAttributes(this TagsCollectionBase tagsCollection)
        {
            if (tagsCollection == null) return null;

            var attributeCollection = new AttributeCollection();
            foreach (var tag in tagsCollection) attributeCollection.AddOrReplace(tag.Key, tag.Value);
            return attributeCollection;
        }
        /// <summary>
        ///     Converts the given tags collection to an attributes collection.
        /// </summary>
        public static IAttributeCollection ToAttributes(this Dictionary<string,string> tagsCollection)
        {
            if (tagsCollection == null) return null;

            var attributeCollection = new AttributeCollection();
            foreach (var tag in tagsCollection) attributeCollection.AddOrReplace(tag.Key, tag.Value);
            return attributeCollection;
        }


        /// <summary>
        /// Adds a tag as an attribute.
        /// </summary>
        public static void Add(this IAttributeCollection attributes, Tag tag)
        {
            attributes.AddOrReplace(tag.Key, tag.Value);
        }

        /// <summary>
        /// Adds or appends the tag value to the value collection.
        /// </summary>
        public static void AddOrAppend(this TagsCollectionBase tags, Tag tag)
        {
            foreach (var t in tags)
            {
                if (t.Key == tag.Key)
                {
                    if (!string.IsNullOrWhiteSpace(t.Value))
                    {
                        var values = t.Value.Split(',');
                        for (var i = 0; i < values.Length; i++)
                        {
                            if (values[i] == tag.Value)
                            {
                                return;
                            }
                        }
                        tags.AddOrReplace(tag.Key, t.Value + "," + tag.Value);
                    }
                    else
                    {
                        tags.AddOrReplace(tag);
                    }
                    return;
                }
            }
            tags.Add(tag);
        }

    }
}