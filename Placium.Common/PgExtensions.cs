using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NDbfReader;
using Newtonsoft.Json;
using Npgsql;
using OsmSharp;
using OsmSharp.Tags;
using Placium.Types;

namespace Placium.Common;

public static class PgExtensions
{
    private const string Null = "null";

    public static string TypeAsText(this IColumn column)
    {
        if (column.Type == typeof(string))
            return $"VARCHAR({column.Size})";
        if (column.Type == typeof(decimal?))
            return $"NUMERIC({column.Size})";
        if (column.Type == typeof(DateTime?))
            return "TIMESTAMP";
        if (column.Type == typeof(int))
            return "INTEGER";
        if (column.Type == typeof(bool))
            return "BOOLEAN";

        throw new NotImplementedException();
    }

    public static string ValueAsText(this IColumn column, Reader reader)
    {
        if (column.Type == typeof(string))
            return reader.GetString(column) != null
                ? TextEscape(reader.GetString(column))
                : string.Empty;
        if (column.Type == typeof(decimal?))
            return reader.GetDecimal(column).HasValue
                ? reader.GetDecimal(column).Value.ToString("G", CultureInfo.InvariantCulture)
                : string.Empty;
        if (column.Type == typeof(DateTime?))
            return reader.GetDateTime(column).HasValue
                ? reader.GetDateTime(column).Value.ToString("u", CultureInfo.InvariantCulture)
                : string.Empty;
        return reader.GetValue(column) != null
            ? reader.GetValue(column).ToString()
            : string.Empty;
    }

    public static string Yo(this string s)
    {
        if (s == null) return string.Empty;

        return s.Replace("ё", "е")
            .Replace("Ё", "Е");
    }

    public static string Escape(this string s)
    {
        if (s == null) return string.Empty;

        return s.Replace("\\", @"\\")
            .Replace("\'", @"\'")
            .Replace("\r", @"\r")
            .Replace("\n", @"\n")
            .Replace("\t", @"\t")
            .Replace("\a", @"\a")
            .Replace("\b", @"\b")
            .Replace("\f", @"\f")
            .Replace("\v", @"\v")
            .Replace("\0", @"\0")
            .Replace("!", @"\!")
            .Replace("$", @"\$")
            .Replace("&", @"\&")
            .Replace("-", @"\-")
            .Replace("<", @"\<")
            .Replace("@", @"\@")
            .Replace("^", @"\^")
            .Replace("|", @"\|")
            .Replace("~", @"\~")
            .Replace("=", @"\=")
            .Replace("/", @"\/")
            .Replace("(", @"\(")
            .Replace(")", @"\)")
            .Replace("\"", @"\""");
    }

    public static string Escape2(this string s)
    {
        if (s == null) return string.Empty;

        return s.Replace("\\", @"\\\\")
            .Replace("\'", @"\\\'")
            .Replace("\r", @"\\\r")
            .Replace("\n", @"\\\n")
            .Replace("\t", @"\\\t")
            .Replace("\a", @"\\\a")
            .Replace("\b", @"\\\b")
            .Replace("\f", @"\\\f")
            .Replace("\v", @"\\\v")
            .Replace("\0", @"\\\0")
            .Replace("!", @"\\\!")
            .Replace("$", @"\\\$")
            .Replace("&", @"\\\&")
            .Replace("-", @"\\\-")
            .Replace("<", @"\\\<")
            .Replace("@", @"\\\@")
            .Replace("^", @"\\\^")
            .Replace("|", @"\\\|")
            .Replace("~", @"\\\~")
            .Replace("=", @"\\\=")
            .Replace("/", @"\\\/")
            .Replace("(", @"\\\(")
            .Replace(")", @"\\\)")
            .Replace("\"", @"\\\""");
    }

    public static string TextEscape(this string s, int mode = 0)
    {
        if (s == null) return string.Empty;

        return mode switch
        {
            0 => s.Replace("\\", @"\\")
                .Replace("\'", @"\'")
                .Replace("\r", @"\r")
                .Replace("\n", @"\n")
                .Replace("\t", @"\t")
                .Replace("\a", @"\a")
                .Replace("\b", @"\b")
                .Replace("\f", @"\f")
                .Replace("\v", @"\v")
                .Replace("\0", @"\0"),
            1 => s.Replace("\\", @"\\")
                .Replace("\"", @"\""")
                .Replace("\'", @"\'")
                .Replace("\r", @"\r")
                .Replace("\n", @"\n")
                .Replace("\t", @"\t")
                .Replace("\a", @"\a")
                .Replace("\b", @"\b")
                .Replace("\f", @"\f")
                .Replace("\v", @"\v")
                .Replace("\0", @"\0"),
            2 => s.Replace("\\", @"\\\\")
                .Replace("\"", @"\\\""")
                .Replace("\'", @"\\\'")
                .Replace("\r", @"\\\r")
                .Replace("\n", @"\\\n")
                .Replace("\t", @"\\\t")
                .Replace("\a", @"\\\a")
                .Replace("\b", @"\\\b")
                .Replace("\f", @"\\\f")
                .Replace("\v", @"\\\v")
                .Replace("\0", @"\\\0"),
            4 => s.Replace("\\", @"\\\\\\\\")
                .Replace("\"", @"\\\\\\\""")
                .Replace("\'", @"\\\\\\\'")
                .Replace("\r", @"\\\\\\\r")
                .Replace("\n", @"\\\\\\\n")
                .Replace("\t", @"\\\\\\\t")
                .Replace("\a", @"\\\\\\\a")
                .Replace("\b", @"\\\\\\\b")
                .Replace("\f", @"\\\\\\\f")
                .Replace("\v", @"\\\\\\\v")
                .Replace("\0", @"\\\\\\\0"),
            _ => throw new NotImplementedException()
        };
    }

    public static string ValueAsText(this bool value)
    {
        return value ? "true" : "false";
    }

    public static string ValueAsText(this double? value)
    {
        return value?.ToString("F6", CultureInfo.InvariantCulture) ?? string.Empty;
    }

    public static string ValueAsText(this double value)
    {
        return value.ToString("F6", CultureInfo.InvariantCulture);
    }

    public static string ValueAsText(this float value)
    {
        return value.ToString("F6", CultureInfo.InvariantCulture);
    }

    public static string ValueAsText(this DateTime? value)
    {
        return value != null ? $"{value.Value.ToString("u", CultureInfo.InvariantCulture)}" : string.Empty;
    }

    public static string ValueAsText(this string value)
    {
        return value != null ? $"{value.TextEscape()}" : string.Empty;
    }

    public static string SafeGetString(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    public static int? SafeGetInt32(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetInt32(ordinal);
    }

    public static long? SafeGetInt64(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);
    }

    public static float? SafeGetFloat(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetFloat(ordinal);
    }

    public static double? SafeGetDouble(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetDouble(ordinal);
    }

    public static DateTime? SafeGetDateTime(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);
    }

    public static bool? SafeGetBoolean(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);
    }

    public static object SafeGetValue(this NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
    }

    public static TagsCollection ToTags(this string s)
    {
        var result = new TagsCollection();
        var dictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(s);
        if (dictionary != null)
            foreach (var pair in dictionary)
                result.AddOrReplace(pair.Key, pair.Value);
        return result;
    }

    public static TagsCollection ToTags(this Dictionary<string, string> dictionary)
    {
        var result = new TagsCollection();
        if (dictionary != null)
            foreach (var pair in dictionary)
                result.AddOrReplace(pair.Key, pair.Value);
        return result;
    }

    public static Node Fill(this Node node, NpgsqlDataReader reader, int offset = 0)
    {
        node.Id = reader.SafeGetInt64(offset + 0);
        node.Version = reader.SafeGetInt32(offset + 1);
        node.Latitude = reader.SafeGetFloat(offset + 2);
        node.Longitude = reader.SafeGetFloat(offset + 3);
        node.ChangeSetId = reader.SafeGetInt64(offset + 4);
        node.TimeStamp = reader.SafeGetDateTime(offset + 5);
        node.UserId = reader.SafeGetInt64(offset + 6);
        node.UserName = reader.SafeGetString(offset + 7);
        node.Visible = reader.SafeGetBoolean(offset + 8);
        node.Tags = (reader.SafeGetValue(offset + 9) as Dictionary<string, string> ??
                     new Dictionary<string, string>())
            .ToTags();
        return node;
    }

    public static Way Fill(this Way way, NpgsqlDataReader reader, int offset = 0)
    {
        way.Id = reader.SafeGetInt64(offset + 0);
        way.Version = reader.SafeGetInt32(offset + 1);
        way.ChangeSetId = reader.SafeGetInt64(offset + 2);
        way.TimeStamp = reader.SafeGetDateTime(offset + 3);
        way.UserId = reader.SafeGetInt64(offset + 4);
        way.UserName = reader.SafeGetString(offset + 5);
        way.Visible = reader.SafeGetBoolean(offset + 6);
        way.Tags = (reader.SafeGetValue(offset + 7) as Dictionary<string, string> ??
                    new Dictionary<string, string>())
            .ToTags();
        way.Nodes = (long[])reader.SafeGetValue(offset + 8);
        return way;
    }

    public static Relation Fill(this Relation relation, NpgsqlDataReader reader, int offset = 0)
    {
        relation.Id = reader.SafeGetInt64(offset + 0);
        relation.Version = reader.SafeGetInt32(offset + 1);
        relation.ChangeSetId = reader.SafeGetInt64(offset + 2);
        relation.TimeStamp = reader.SafeGetDateTime(offset + 3);
        relation.UserId = reader.SafeGetInt64(offset + 4);
        relation.UserName = reader.SafeGetString(offset + 5);
        relation.Visible = reader.SafeGetBoolean(offset + 6);
        relation.Tags = (reader.SafeGetValue(offset + 7) as Dictionary<string, string> ??
                         new Dictionary<string, string>())
            .ToTags();
        relation.Members = (reader.SafeGetValue(offset + 8) as OsmRelationMember[] ?? new OsmRelationMember[0])
            .Select(x => new RelationMember(x.Id, x.Role, (OsmGeoType)x.Type)).ToArray();
        return relation;
    }
}