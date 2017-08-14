using MyORM;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

public static class Modulo
{

    public static string connectionString = "";

    public static Server DataBaseServer = Server.MYSQLSERVER;

    public static T Find<T>(int id)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Find<T>(id);
    }

    public static List<T> Consulta<T>()
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Entities<T>().ToList();
    }
    public static List<T> Consulta<T>(string cons)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Consulta<T>(cons);
    }

    public static List<T> Consulta<T>(Expression<Func<T, bool>> exp)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Consulta(exp);
    }

    public static object Consulta(Type type, string cons)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Consulta(type, cons);
    }


    public static List<T> Consulta<T>(string cons, CommandType tp)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Consulta<T>(cons, tp);
    }


    public static List<T> Consulta<T>(IDbCommand cmd)
    {
        DbContext db = new DbContext(connectionString, DataBaseServer);
        return db.Consulta<T>(cmd);
    }

    public static T CrearTipo<T>()
    {

        return (T)CrearTipo(typeof(T));
    }

    public static object CrearTipo(this Type tipo)
    {

        if (tipo.Name == "String")
            return Activator.CreateInstance(tipo, new object[] { new char[] { } });
        else if (tipo.IsValueType)
            return Activator.CreateInstance(tipo);
        else if (tipo.Name.Contains("<>"))
            return null;
        else
            return Activator.CreateInstance(tipo);


    }


    public static string GetTableName<T>()
    {
        return GetTableName(typeof(T));
    }

    public static string GetTableName(this Type tipo)
    {
        Table[] MyAttributes = (Table[])Attribute.GetCustomAttributes(tipo, typeof(Table));

        if (MyAttributes == null || MyAttributes.Length <= 0) return tipo.Name;
        return MyAttributes[0].Name;
    }

    public static string GetColumnName(this PropertyInfo property)
    {
        Column MyAttribute = (Column)Attribute.GetCustomAttributes(property, false).FirstOrDefault(t => t is Column);
        if (MyAttribute == null) return property.Name;
        return MyAttribute.Name;
    }

    public static string GetColumnName<T>(string Name)
    {
        var property = typeof(T).GetProperty(Name);

        if (property == null) return Name;

        return property.GetColumnName();
    }

    public static string GetColumnName(Type tipo, string Name)
    {
        var property = tipo.GetProperty(Name);

        if (property == null) return Name;

        return property.GetColumnName();
    }


    public static T Map<T>(object obj)
    {
        object result = null;
        if (obj == null) return (T)result;

        Type typeResult = typeof(T);
        Type type = obj.GetType();

        result = Activator.CreateInstance<T>();

        foreach (var property in type.GetProperties())
        {
            var pResult = typeResult.GetProperty(property.Name);
            if (pResult == null) continue;

            pResult.SetValue(result, property.GetValue(obj, null), null);
        }

        return (T)result;
    }


    public static T Find<T, Y>(this IQueryable<T> entities, object id, Expression<Func<T, Y>> select)
    {
        Type tipo = typeof(T);
        string tableName = Modulo.GetTableName<T>();
        var p = tipo.GetProperties().FirstOrDefault(a => a.GetCustomAttributes(typeof(PrimaryKey), true).Length > 0);

        if (p == null) throw new MemberAccessException("El objeto no posee una llave primaria");

        var param = Expression.Parameter(typeof(T), "e");
        var predicate = Expression.Lambda<Func<T, Boolean>>
                        (Expression.Equal(Expression.Property(param, p.GetColumnName()),
                                          Expression.Constant(id)),
                                          new ParameterExpression[] { param });

        
        if (select != null)
           return Map<T>(entities.Where(predicate).Select(select).FirstOrDefault());
        else
           return entities.FirstOrDefault(predicate);
    }

    public static T Find<T, Y>(this IQueryable<T> entities, Expression<Func<T, bool>> where, Expression<Func<T, Y>> select)
    {
        if (select != null)
            return Map<T>(entities.Where(where).Select(select).FirstOrDefault());
        else
            return entities.FirstOrDefault(where);
    }
}