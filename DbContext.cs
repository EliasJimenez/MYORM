using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Collections;
using System.Linq.Expressions;
using System.Data.OracleClient;
using MyORM;
using static Modulo;
using System.Text.RegularExpressions;

delegate Result Where<T, out Result>(T obj);
public class DbContext
{
    public string connectionString = "";

    public Server DataBaseServer = Server.MYSQLSERVER;
    public IDbConnection con = null;
    List<KeyValuePair<String, String>> _mapping = new List<KeyValuePair<String, String>>();
    int depht = 0;

    public DbContext()
    {

    }
    public DbContext(String ConnectionString, Server DataBaseServer)
    {
        this.connectionString = ConnectionString;
        this.DataBaseServer = DataBaseServer;
    }

    public IDBQuery GetDBQuery()
    {
        if (DataBaseServer == Server.MSSQLSERVER)
            return new DBQuerySQLSERVER();
        else if (DataBaseServer == Server.MYSQLSERVER)
            return new DBQueryMySQL();
        else if (DataBaseServer == Server.ORACLESERVER)
            return new DBQuerySQLSERVER();

        return new DBQuerySQLSERVER();
    }

    IDbConnection GetConnection(string connection)
    {
        if (con != null) return con;

        if (DataBaseServer == Server.MSSQLSERVER)
            return new SqlConnection(connection);
        else if (DataBaseServer == Server.MYSQLSERVER)
            return new MySqlConnection(connection);
        else if (DataBaseServer == Server.ORACLESERVER)
            return new OracleConnection(connection);

        return new SqlConnection(connection);
    }

    IDbCommand GetCommand()
    {
        if (DataBaseServer == Server.MSSQLSERVER)
            return new SqlCommand();
        else if (DataBaseServer == Server.MYSQLSERVER)
            return new MySqlCommand();
        else if (DataBaseServer == Server.ORACLESERVER)
            return new OracleCommand();

        return new SqlCommand();
    }

    IDbDataParameter GetParameter(string name, object value, bool IsNullable = false)
    {
        if (DataBaseServer == Server.MSSQLSERVER)
        {
            var parameter = new SqlParameter(name, value);
            if (value == null)
            {
                parameter.IsNullable = true;
                parameter.SourceColumnNullMapping = true;
            }
            return parameter;
        }
        else if (DataBaseServer == Server.MYSQLSERVER)
        {
            var parameter = new MySqlParameter(name, value);
            if (value == null)
            {
                parameter.IsNullable = true;
                parameter.SourceColumnNullMapping = true;
            }
            return parameter;
        }
        else if (DataBaseServer == Server.ORACLESERVER)
        {
            var parameter = new OracleParameter(name, value);
            if (value == null)
            {

                parameter.IsNullable = true;
                parameter.SourceColumnNullMapping = true;
            }
            return parameter;
        }

        return new SqlParameter(name, value) { IsNullable = IsNullable };
    }
    IEnumerable<string> GetColumns(IDataReader reader)
    {
        int cantColumnas = reader.FieldCount;
        List<string> _columnas = new List<string>();

        for (int a = 0; a < cantColumnas; a++)
            _columnas.Add(reader.GetName(a));

        return _columnas;
    }

    public List<KeyValuePair<string, string>> Mapping
    {
        get
        {
            return this._mapping;
        }
        set
        {
            if (value != null)
                this._mapping = value;

        }
    }

    public void MapToQuery<T>(Expression<Func<T, object>> exp, string ColumnSource)
    {
        var parametro = Regex.Match(exp.ToString(), @"(\w+).").Groups[0].Value.Trim();
        var value = Regex.Match(exp.ToString(), @"\(([^)]*)\)")
          .Groups[1].Value
          .Replace(parametro + ".", "")
          .Trim();

        if (String.IsNullOrEmpty(value))
        {
            value = exp.ToString()
                .Replace(parametro + ".", "")
                .Replace(parametro + " =>", "")
                .Replace("{", "")
                .Replace("}", "")
                .Replace("(", "")
                .Replace(")", "")
                .Trim();
        }

        Mapping.Add(new KeyValuePair<string, string>(ColumnSource, value));
    }

    /// <summary>
    /// Llena las propiedades del objeto que coincidan con los campos retornados en el DataReader
    /// </summary>
    /// <param name="reader">Objeto que contiene los datos</param>
    /// <param name="obj">Objeto donde seran seteados los valores</param>
    /// <returns>Returna un valor que indica si se completo con exito la operacion</returns>
    public bool FillObject(IDataReader reader, object obj)
    {
        try
        {
            if (reader.IsClosed)
                return false;

            var propiedades = obj.GetType().GetProperties();
            bool tieneReg = reader.Read();

            if (tieneReg)
            {
                foreach (PropertyInfo p in propiedades)
                {
                    try
                    {
                        object val = null;
                        Type tipo = p.PropertyType;

                        if (p.PropertyType.IsGenericType)
                        {
                            tipo = p.PropertyType.GetGenericArguments()[0];
                        }

                        val = Convert.ChangeType(reader[p.Name], tipo);


                        p.SetValue(obj, val, null);
                    }
                    catch { }

                }
            }
            return true;
        }
        catch (Exception)
        {

            return false;
        }

    }

    public T FillObject<T>(IDataReader reader)
    {
        var obj = Modulo.CrearTipo<T>();
        FillObject(reader, obj);
        return obj;
    }

    public IList ListaObject(Type tobj, IDataReader reader)
    {
        Type listType = typeof(List<>).MakeGenericType(new[] { tobj });
        IList lista = (IList)Activator.CreateInstance(listType);


        if (reader.IsClosed)
            return lista;

        bool isAnonimo = (tobj.Name.Length > 1 && tobj.Name.Substring(0, 2) == "<>");
        var columnas = GetColumns(reader);


        var propiedades = tobj.GetProperties();
        while (reader.Read())
        {
            depht = 0;
            object obj = tobj.CrearTipo();

            if (tobj.IsValueType || tobj.Name == "String")
            {
                if (tobj.IsGenericType)
                    tobj = tobj.GetGenericArguments()[0];

                if (reader[0] != DBNull.Value)
                    obj = Convert.ChangeType(reader[0], tobj);
                lista.Add(obj);
                continue;
            }
            else if (isAnonimo)
            {
                var paramteros = tobj.GetConstructors()[0].GetParameters();
                object[] param = new object[paramteros.Length];

                for (int i = 0; i < paramteros.Length; i++)
                {
                    if (paramteros[i].ParameterType.IsClass && paramteros[i].ParameterType != typeof(string))
                    {
                        object _obj = Activator.CreateInstance(paramteros[i].ParameterType);
                        bool _seted = SetValue(paramteros[i].Name, paramteros[i].ParameterType.GetProperties(), reader, _obj, columnas);
                        param[i] = _seted ? _obj : null;
                        continue;
                    }

                    string fullColumnName = _mapping.FirstOrDefault(t => t.Value == $"{paramteros[i].Name}").Key;
                    fullColumnName = fullColumnName ?? paramteros[i].Name;

                    if (columnas.Any(t => string.Equals(t, fullColumnName, StringComparison.InvariantCultureIgnoreCase)))
                    {
                        var _val = reader[fullColumnName];
                        if (_val is DBNull || _val == DBNull.Value) continue;
                        var _type = paramteros[i].ParameterType;
                        if (_type.IsGenericType) _type = _type.GetGenericArguments()[0];
                        param[i] = Convert.ChangeType(_val, _type);
                    }
                }

                obj = Activator.CreateInstance(tobj, param);
                lista.Add(obj);
                continue;
            }


            SetValue("", propiedades, reader, obj, columnas);

            lista.Add(obj);
        }

        return lista;
    }

    public bool SetValue(string root, PropertyInfo[] propiedades, IDataReader reader, object obj, IEnumerable<string> columnas)
    {
        bool seted = false;
        foreach (PropertyInfo p in propiedades)
        {
            try
            {
                object val = null;
                Type tipo = p.PropertyType;
                string separador = string.IsNullOrEmpty(root) ? "" : ".";
                string columnName = p.GetColumnName();
                string fullColumnName = _mapping.FirstOrDefault(t => t.Value == $"{root}{separador}{columnName}").Key;


                columnName = (fullColumnName != null) ? fullColumnName : columnName;

                if (p.PropertyType.IsInterface && p.PropertyType.IsGenericType)
                {
                    CrearIQueryProperty(obj, p);
                    continue;
                }
                else if (tipo.IsClass && tipo.Name != "String")
                {
                    depht++;
                    object newObj = Activator.CreateInstance(tipo);
                    p.SetValue(obj, newObj, null);
                    bool _seted = SetValue($"{root}{separador}{p.Name}", tipo.GetProperties(), reader, newObj, columnas);

                    if (!_seted)
                        p.SetValue(obj, null, null);
                    continue;
                }
                else if (depht > 0 && fullColumnName == null) continue;
                else if (p.PropertyType.IsGenericType)
                {
                    tipo = p.PropertyType.GetGenericArguments()[0];
                }

                if (columnas.Any(t => string.Equals(t, columnName, StringComparison.InvariantCultureIgnoreCase)))
                {
                    var _val = reader[columnName];
                    if (_val is DBNull || _val == DBNull.Value) continue;
                    val = Convert.ChangeType(_val, tipo);
                    seted = true;
                }


                p.SetValue(obj, val, null);
            }
            catch (Exception ex) { }

        }
        return seted;
    }

    private void CrearIQueryProperty(object obj, PropertyInfo p)
    {
        var provider = new MyQueryProvider(this);



        Join MyAttribute = (Join)Attribute.GetCustomAttributes(p, false).FirstOrDefault(t => t is Join);

        if (MyAttribute == null) return;

        PropertyInfo pJoin1 = obj.GetType().GetProperties().FirstOrDefault(t => t.GetCustomAttributes(typeof(Column), true).Length > 0 && ((Column)t.GetCustomAttributes(typeof(Column), true)[0]).Name == MyAttribute.Key);
        if (pJoin1 == null) pJoin1 = obj.GetType().GetProperty(MyAttribute.Key);
        object pJoinValue1 = pJoin1.GetValue(obj, null);

        Func<object, bool> g;
        var tipoBase = p.PropertyType.GetGenericArguments()[0];
        var delegado = typeof(Func<,>).MakeGenericType(tipoBase, typeof(bool));
        var delegadoExpresion = typeof(Expression<>).MakeGenericType(delegado);
        var param = Expression.Parameter(tipoBase, "e");
        var predicate = LambdaExpression.Lambda(delegado, Expression.Equal(Expression.Property(param, MyAttribute.ForeignKey),
                                          Expression.Constant(pJoinValue1)),
                                          new ParameterExpression[] { param });


        Type _tipo = typeof(MyQueryable<>).MakeGenericType(tipoBase);
        var query = (IQueryable)Activator.CreateInstance(_tipo, provider, this);

        query = query.Provider.CreateQuery(
               Expression.Call(
                   typeof(Queryable), "Where",
                   new Type[] { tipoBase },
                   query.Expression, Expression.Quote(predicate)));

        p.SetValue(obj, query, null);
    }

    public T Find<T>(object id)
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



        //return Consulta<T>(string.Format("select * from {0} where {1}={2}", tableName, p.GetColumnName(), id)).FirstOrDefault();
       return  Entities<T>().FirstOrDefault(predicate);
    }



    public IQueryable<T> Entities<T>()
    {
        var provider = new MyQueryProvider(this);
        var query = new MyQueryable<T>(provider, this);
        return query;
    }

    /// <summary>
    /// Ejecuta un query y retorna los datos con el tipo especificado
    /// </summary>
    /// <typeparam name="T">Tipo de objeto que sera retornado</typeparam>
    /// <param name="cons">Query que contiene la instruccion</param>
    /// <returns>Returna una lista de objetos</returns>

    public List<T> Consulta<T>(string cons)
    {

        IDbCommand cmd = GetCommand();
        cmd.CommandText = cons;
        return Consulta<T>(cmd);
    }

    public List<T> Consulta<T>(Expression<Func<T, bool>> exp)
    {
        //string tableName = GetTableName<T>();
        //string textCommand = new QueryTranslator(typeof(T),GetDBQuery()).Translate(exp);
        //IDbCommand cmd = GetCommand();
        //cmd.CommandText = string.Format("select * from {0} where {1}", tableName, textCommand);
        //return Consulta<T>(cmd);

        return Entities<T>().Where(exp).ToList();
    }

    public object Consulta(Type _type, string cons)
    {

        IDbConnection con = GetConnection(connectionString);
        var cmd = GetCommand();
        cmd.CommandText = cons;
        cmd.Connection = con;
        bool debeCerrarConn = true;
        if (con.State == ConnectionState.Closed)
            con.Open();
        else
            debeCerrarConn = false;

        var result = cmd.ExecuteReader();
        var lista = ListaObject(_type, result);

        if (debeCerrarConn)
            con.Close();
        return lista;
    }

    /// <summary>
    /// Ejecuta un query y retorna los datos con el tipo especificado
    /// </summary>
    /// <typeparam name="T">Tipo de objeto que sera retornado</typeparam>
    /// <param name="cons">Query que contiene la instruccion</param>
    /// <param name="tp">Tipo de comando que sera ejecutado</param>
    /// <returns>Returna una lista de objetos</returns>
    public List<T> Consulta<T>(string cons, CommandType tp)
    {
        IDbCommand cmd = GetCommand();
        cmd.CommandType = tp;
        cmd.CommandText = cons;
        return Consulta<T>(cmd);
    }

    /// <summary>
    /// Ejecuta un query y retorna los datos con el tipo especificado
    /// </summary>
    /// <typeparam name="T">Tipo de objeto que sera retornado</typeparam>
    /// <param name="cmd">Objeto Command que ejecuta la instruccion</param>
    /// <returns>Returna una lista de objetos</returns>
    public List<T> Consulta<T>(IDbCommand cmd)
    {
        IDbConnection con = GetConnection(connectionString);
        cmd.Connection = con;

        bool debeCerrarConn = true;
        if (con.State == ConnectionState.Closed)
            con.Open();
        else
            debeCerrarConn = false;

        var result = cmd.ExecuteReader();
        var lista = (List<T>)ListaObject(typeof(T), result);

        if (debeCerrarConn)
            con.Close();
        return lista;
    }


    public T Insert<T>(T obj)
    {
       return  Insert<T>(obj, false);
    }
    public T Insert<T>(T obj,bool Refresh)
    {
        Type tipo = typeof(T);
        string tableName = GetTableName<T>();
        PropertyInfo llave = null;
        PropertyInfo identity = null;
        object valueLlave = null;
        var con = GetConnection(connectionString);
        var command = GetCommand();
        command.Connection = con;

        string query = $"insert into {tableName} ({string.Join(",", GetColumnsObject(obj))})";
        string queryValues = " values (";

        string queryReturn = "Select * from " + tableName;
        queryReturn += " where ";

        foreach (var property in tipo.GetProperties())
        {
            if (property.PropertyType.IsClass && property.PropertyType != typeof(String))
                continue;

            if (property.GetCustomAttributes(typeof(PrimaryKey), true).Length > 0)
            {
                llave = property;
                valueLlave = property.GetValue(obj, null);
            }


            if (property.GetCustomAttributes(typeof(AutoGenerated), true).Length > 0)
            {
                identity = property.Name == llave.Name ? property : identity;
                continue;
            }
            else if (property.GetCustomAttributes(typeof(AutoGeneratedNoDB), true).Length > 0)
            {
                continue;
            }


            string columnName = property.GetColumnName();
            object columnValue = property.GetValue(obj, null);

            if (columnValue != null)
            {
                queryValues += string.Format("@{0},", columnName);
                command.Parameters.Add(GetParameter("@" + columnName, columnValue));
            }
            else
            {

            }

        }
        queryValues = queryValues.Substring(0, queryValues.Length - 1) + "); SELECT @@identity AS id;";

        command.CommandText = query + queryValues;

        bool debeCerrarConn = true;
        if (con.State == ConnectionState.Closed)
            con.Open();
        else
            debeCerrarConn = false;

        int id = int.Parse(command.ExecuteScalar().ToString());

        if (debeCerrarConn)
            con.Close();

     

        if (id > 0 && llave != null && identity != null && llave.Name == identity.Name)
        {
            // queryReturn += string.Format("{0}={1};", llave.GetColumnName(), id);
            llave.SetValue(obj, id, null);
            if (!Refresh)
                return obj;

            var _obj = Find<T>(id);
            obj = _obj != null ? _obj : obj;
        }
        else if (llave != null && identity == null)
        {
            if (!Refresh)
                return obj;

            var _obj = this.Find<T>(valueLlave);
            obj = _obj != null ? _obj : obj;
        }


        return obj;
    }

    private IEnumerable<string> GetColumnsObject(object obj)
    {
        Type tipo = obj.GetType();
        List<string> columns = new List<string>();
        foreach (PropertyInfo property in tipo.GetProperties())
        {
            if (property.GetValue(obj, null) == null) continue;
            if (property.GetCustomAttributes(typeof(AutoGenerated), true).Any() || property.GetCustomAttributes(typeof(AutoGeneratedNoDB), true).Any()) continue;
            if (property.PropertyType.IsClass && property.PropertyType != typeof(string)) continue;
            columns.Add(property.GetColumnName());
        }

        return columns;
    }
    public bool Update<T>(T obj)
    {
        return Update(obj, new string[] { });
    }

    public bool Update<T>(T obj, Func<T, object> properties)
    {
        var _prperties = properties.Invoke(obj).GetType().GetProperties().Select(t => t.Name);
        return Update(obj, _prperties);
    }

    public bool Update<T>(T obj, IEnumerable<string> fields)
    {
        string tableName = GetTableName<T>();
        return Update(obj, tableName, fields);
    }

    public bool Update<Table, TObject>(TObject obj)
    {
        string tableName = GetTableName<Table>();
        return Update(obj, tableName, new string[] { });
    }
    public bool Update<T>(T obj, string tableName, IEnumerable<string> fields)
    {
        Type tipo = typeof(T);

        var con = GetConnection(connectionString);
        var command = GetCommand();
        command.Connection = con;

        string query = string.Format("update {0} set ", tableName);
        string queryValues = " where ";
        object valueKey = null;
        foreach (var property in tipo.GetProperties())
        {
            string columnName = property.GetColumnName();

            if (property.PropertyType.IsInterface || property.PropertyType.IsClass && property.PropertyType != typeof(String))
                continue;

            if (property.GetCustomAttributes(typeof(PrimaryKey), true).Length > 0)
            {
                queryValues += string.Format("{0}=@{0} and ", columnName);
                command.Parameters.Add(GetParameter("@" + columnName, property.GetValue(obj, null)));
                valueKey = property.GetValue(obj, null);
                continue;
            }
            if (property.GetCustomAttributes(typeof(AutoGenerated), true).Length > 0 || property.GetCustomAttributes(typeof(AutoGeneratedNoDB), true).Length > 0)
                continue;

            if (fields != null && fields.Count() > 0 && !fields.Contains(columnName))
                continue;

            object value = property.GetValue(obj, null);

            if (value != null)
            {
                query += string.Format("{0}=@{0},", columnName);
                command.Parameters.Add(GetParameter("@" + columnName, value));
            }
            else
                query += string.Format("{0}=NULL,", columnName);
        }

        query = query.Substring(0, query.Length - 1) + "";
        queryValues = queryValues.Substring(0, queryValues.Length - 4) + ";";

        command.CommandText = query + queryValues;
        bool debeCerrarConn = true;
        if (con.State == ConnectionState.Closed)
            con.Open();
        else
            debeCerrarConn = false;

        command.ExecuteNonQuery();
        if (debeCerrarConn)
            con.Close();
        return true;
    }

    public void Delete<T>(int id)
    {
        Type tipo = typeof(T);
        string tableName = GetTableName<T>();
        var p = tipo.GetProperties().FirstOrDefault(a => a.GetCustomAttributes(typeof(PrimaryKey), true).Length > 0);

        if (p == null) throw new MemberAccessException("El objeto no posee una llave primaria");
        Consulta<T>(string.Format("delete from {0} where {1}={2}", tableName, p.GetColumnName(), id)).FirstOrDefault();
    }

    public void Delete<T>(T obj)
    {
        Type tipo = typeof(T);
        string tableName = GetTableName<T>();
        var p = tipo.GetProperties().FirstOrDefault(a => a.GetCustomAttributes(typeof(PrimaryKey), true).Length > 0);

        if (p == null) throw new MemberAccessException("El objeto no posee una llave primaria");
        Consulta<T>(string.Format("delete from {0} where {1}={2}", tableName, p.GetColumnName(), p.GetValue(obj, null))).FirstOrDefault();
    }

    protected T Map<T>(object obj)
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

}



