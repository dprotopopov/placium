using System;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Reflection;
using Microsoft.AspNetCore.Connections;

namespace AstraUtils;

public class FieldAccessor<TObject, TField>
{
    private Func<TObject, TField> _func;

    public FieldAccessor(string field_name)
    {
        _func = info =>
        {
            if (TryCreateFunc(info, field_name, out var result))
            {
                _func = result;

                return _func(info);
            }

            _func = x => default;

            return default;
        };
    }

    private static bool TryCreateFunc(TObject obj, string field_name, out Func<TObject, TField> func)
    {
        var type = obj.GetType();


        var field = type.GetField(field_name, BindingFlags.Instance | BindingFlags.NonPublic);
        if (field == null || !typeof(Socket).IsAssignableFrom(field.FieldType))
        {
            func = null;
            return false;
        }

        var param = Expression.Parameter(typeof(ConnectionContext), "x");

        var lambda = Expression.Lambda<Func<TObject, TField>>(
            Expression.Condition(Expression.TypeIs(param, type),
                Expression.Field(Expression.Convert(param, type), field),
                Expression.Constant(null, typeof(TField)),
                typeof(TField)),
            param);


        func = lambda.Compile();
        return true;
    }

    public TField GetValue(TObject obj)
    {
        return _func(obj);
    }
}