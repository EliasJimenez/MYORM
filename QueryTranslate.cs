using MyORM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
public class QueryTranslator : ExpressionVisitor
{
    private StringBuilder sb;
    private string _orderBy = string.Empty;
    private int? _skip = null;
    private int? _take = null;
    private List<string> _whereClause = new List<string>();
    private List<string> _selectClause = new List<string>();
    private int _selectIndex = 0;
    private bool hasWhere = false;
    private Type tipo = null;
    private List<Expression> lsExpressios = new List<Expression>();
    private int expressionIndex = 0;
    private IDBQuery queryCreator = null;
    public List<KeyValuePair<string, string>> Mapping = new List<KeyValuePair<string, string>>();
    MyQuerybuilder MQB = null;
    bool primerSelect = true;
    List<Field> camposSelect = new List<Field>();
    string parameterWhere = "";
    bool _parsingWhere = false;
    public int? Skip
    {
        get
        {
            return _skip;
        }
    }

    public int? Take
    {
        get
        {
            return _take;
        }
    }

    public string OrderBy
    {
        get
        {
            return _orderBy;
        }
    }


    public QueryTranslator(Type tipo, IDBQuery queryCreator)
    {
        this.tipo = tipo;
        this.queryCreator = queryCreator;
    }

    public string Translate(Expression expression)
    {
        lsExpressios.Clear();
        Mapping.Clear();


        this.sb = new StringBuilder();
        var primeraExp = FirstExprresion(expression);

        tipo = ((ConstantExpression)primeraExp).Type.GetGenericArguments()[0];
        MQB = new MyORM.MyQuerybuilder(tipo);

        string tableName = tipo.GetTableName();
        string _tableName = "(" + tableName + ")";


        if (lsExpressios.Count > 0)
            this.Visit(lsExpressios[expressionIndex]);


        sb.Clear();

        foreach (var f in MQB.lstSelect)
            Mapping.Add(new KeyValuePair<string, string>(f.DbAlias, f.TypeField));

        //string where = sb.Length > 0 ? sb.ToString() : "";
        _selectClause.Add(MQB.ToString());

        string query = _selectClause.LastOrDefault();


        return query ?? $"Select * from {tipo.GetTableName()} t{_selectIndex}";
    }

    private static Expression StripQuotes(Expression e)
    {
        while (e.NodeType == ExpressionType.Quote)
        {
            e = ((UnaryExpression)e).Operand;
        }
        return e;
    }

    protected override Expression VisitMethodCall(MethodCallExpression m)
    {
        if (_selectClause.Count <= 0)
        {

            camposSelect = MQB.lstSelect;
        }


        if (m.Method.DeclaringType == typeof(Queryable) && (m.Method.Name == "Where" || m.Method.Name == "FirstOrDefault"))
        {
            _parsingWhere = true;


            if (m.Arguments.Count > 1)
            {
                LambdaExpression lambda = (LambdaExpression)StripQuotes(m.Arguments[1]);
                VisitWhere(lambda.Body);
            }

            expressionIndex++;
            if (expressionIndex == lsExpressios.Count)
                return null;
            return this.Visit(lsExpressios[expressionIndex]);

        }

        else if (m.Method.Name == "Take")
        {
            if (this.ParseTakeExpression(m))
            {
                expressionIndex++;
                if (expressionIndex == lsExpressios.Count)
                    return null;

                return this.Visit(lsExpressios[expressionIndex]);

            }
        }
        else if (m.Method.Name == "Count")
        {
            if (m.Arguments.Count > 1)
            {
                LambdaExpression lambda = (LambdaExpression)StripQuotes(m.Arguments[1]);
                VisitWhere(lambda.Body);
            }

            MQB.Count = true;
            expressionIndex++;
            if (expressionIndex == lsExpressios.Count)
                return null;

            return this.Visit(lsExpressios[expressionIndex]);

        }
        else if (m.Method.Name == "Any")
        {
            if (m.Arguments.Count > 1)
            {
                LambdaExpression lambda = (LambdaExpression)StripQuotes(m.Arguments[1]);
                VisitWhere(lambda.Body);
            }

            MQB.Any = true;
            expressionIndex++;
            if (expressionIndex == lsExpressios.Count)
                return null;

            return this.Visit(lsExpressios[expressionIndex]);

        }
        else if (m.Method.Name == "Max" || m.Method.Name == "Min")
        {
            if (m.Arguments.Count > 1)
            {
                var operand = (LambdaExpression)((UnaryExpression)m.Arguments[1]).Operand;
                if (operand.Body.NodeType == ExpressionType.MemberAccess)
                {
                    var _body = (MemberExpression)operand.Body;

                    string name = GetMember("", operand.Parameters[0].Type, _body);
                    var fieldMax = MQB.lstFields.FirstOrDefault(t => t.TypeField == name);
                    MQB.lstSelect = new List<Field>();
                    MQB.lstSelect.Add(new Field("", $"{m.Method.Name}({fieldMax.Table}.{fieldMax.DbField})", "", "TMP "));
                }

            }

            expressionIndex++;
            if (expressionIndex == lsExpressios.Count)
                return null;

            return this.Visit(lsExpressios[expressionIndex]);

        }
        else if (m.Method.Name == "Skip")
        {
            if (this.ParseSkipExpression(m))
            {
                Expression nextExpression = m.Arguments[0];
                return this.Visit(nextExpression);
            }
        }
        else if (m.Method.Name == "OrderBy")
        {
            if (this.ParseOrderByExpression(m, "ASC"))
            {
                Expression nextExpression = m.Arguments[0];
                return this.Visit(nextExpression);
            }
        }
        else if (m.Method.Name == "OrderByDescending")
        {
            if (this.ParseOrderByExpression(m, "DESC"))
            {
                Expression nextExpression = m.Arguments[0];
                return this.Visit(nextExpression);
            }
        }
        else if (m.Method.Name == "Select")
        {
            var operand = (LambdaExpression)((UnaryExpression)m.Arguments[1]).Operand;

            if (operand.Body.NodeType == ExpressionType.Parameter)
            {
                var _body = (ParameterExpression)operand.Body;
                SelectParameter(_body);
            }
            else if (operand.Body.NodeType == ExpressionType.New)
            {
                var _body = (NewExpression)operand.Body;
                SelectNew(operand.Parameters[0].Type, _body);
            }
            else if (operand.Body.NodeType == ExpressionType.MemberAccess)
            {
                var _body = (MemberExpression)operand.Body;
                SelectMember(operand.Parameters[0].Type, _body);
            }

            expressionIndex++;
            if (expressionIndex == lsExpressios.Count)
                return null;

            return this.Visit(lsExpressios[expressionIndex]);
        }

        throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
    }

    private void VisitWhere(Expression expression)
    {

        if (expression.NodeType == ExpressionType.And ||
             expression.NodeType == ExpressionType.AndAlso ||
             expression.NodeType == ExpressionType.Or ||
             expression.NodeType == ExpressionType.OrElse)
        {
            BinaryExpression _expression = (BinaryExpression)expression;
            string operador = this.operador(_expression.NodeType, _expression);
            MQB.lstWhere.Add(new MyORM.Field() { BinaryType = "(" });
            VisitWhere(_expression.Left);
            MQB.lstWhere.Add(new MyORM.Field() { BinaryType = ")" });

            MQB.lstWhere.Add(new MyORM.Field() { BinaryType = operador });

            MQB.lstWhere.Add(new MyORM.Field() { BinaryType = "(" });
            VisitWhere(_expression.Right);
            MQB.lstWhere.Add(new MyORM.Field() { BinaryType = ")" });

        }
        else
            VisitWhereCompnonent(expression);

    }

    private void VisitWhereCompnonent(Expression expression)
    {
        Field whereField = new Field();
        MQB.lstWhere.Add(whereField);
        BinaryExpression _expression = (BinaryExpression)expression;
        whereField.Operator = operador(_expression.NodeType, _expression);

        VisitWhereMember(_expression.Left, whereField, true);
        VisitWhereMember(_expression.Right, whereField, false);

    }

    private void VisitWhereMember(Expression expression, Field _field, bool left)
    {

        if (expression.NodeType == ExpressionType.MemberAccess)
        {
            MemberExpression _expression = (MemberExpression)expression;
            Expression parameter = null;
            string typeField = GetMember("", this.tipo, _expression, out parameter);

            if (parameter.NodeType == ExpressionType.Parameter)
            {
                var field = MQB.lstFields.FirstOrDefault(t => t.TypeField == typeField);
                if (left)
                {
                    _field.Table = field.Table;
                    _field.DbField = field.DbField;
                }
                else
                    _field.Value = $"{field.Table}.{field.DbField}";

                return;
            }
        }


        if (expression.NodeType == ExpressionType.Constant ||
            expression.NodeType == ExpressionType.MemberAccess ||
            expression.NodeType == ExpressionType.Convert)
        {
            LambdaExpression lambda = Expression.Lambda(expression);

            Delegate fn = lambda.Compile();
            var ConstantExp = Expression.Constant(fn.DynamicInvoke(null), expression.Type);
            string value = ConstantValue(ConstantExp).ToString();

            if (left)
                _field.DbField = value;
            else
                _field.Value = value;

            return;
        }

        if (expression.NodeType == ExpressionType.New)
        {
            string value = NewValue((NewExpression)expression).ToString();
            if (left)
                _field.DbField = value;
            else
                _field.Value = value;

            return;
        }

        //throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
    }

    private string operador(ExpressionType type, BinaryExpression b)
    {
        switch (type)
        {
            case ExpressionType.And:
                return (" AND ");

            case ExpressionType.AndAlso:
                return (" AND ");

            case ExpressionType.Or:
                return (" OR ");

            case ExpressionType.OrElse:
                return (" OR ");

            case ExpressionType.Equal:
                if (IsNullConstant(b.Right))
                {
                    return (" IS ");
                }
                else
                {
                    return (" = ");
                }

            case ExpressionType.NotEqual:
                if (IsNullConstant(b.Right))
                {
                    return (" IS NOT ");
                }
                else
                {
                    return (" <> ");
                }

            case ExpressionType.LessThan:
                return (" < ");


            case ExpressionType.LessThanOrEqual:
                return (" <= ");

            case ExpressionType.GreaterThan:
                return (" > ");
            case ExpressionType.GreaterThanOrEqual:
                return (" >= ");

            default:
                throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));

        }
    }

    string findSimbol(string exp)
    {
        string[] simbolos = { "<=", ">=", "<>", "!=", "IS NOT", "=", "<", ">", "IS" };
        return simbolos.FirstOrDefault(t => exp.Contains(t)) ?? "";
    }

    public void SelectMember(Type _tipo, MemberExpression arg)
    {
        if (primerSelect)
        {

            primerSelect = false;
            List<Field> campos = new List<Field>();



            if (arg.Type.IsClass && arg.Type != typeof(string))
            {
                GetProperties(arg.Member.Name, arg.Type, campos);
            }
            else
            {
                string name = GetMember("", _tipo, arg);
                campos.Add(new Field("$Member", "", name, arg.Member.Name));
            }


            //
            //GetProperties("", expression.Type, campos);
            var result = (from a in MQB.lstFields
                          join b in campos on a.TypeField equals b.TypeField
                          select new Field(a.Table, a.DbField, b.Table == "$Member" ? b.DbAlias : a.TypeField.Substring(arg.Member.Name.Length + 1), a.DbAlias) { }).ToList();

            MQB.lstSelect = result;
        }
    }

    public void SelectNew(Type _tipo, NewExpression expression)
    {
        if (primerSelect)
        {

            primerSelect = false;
            List<Field> campos = new List<Field>();

            for (int i = 0; i < expression.Arguments.Count; i++)
            {
                if (expression.Arguments[i].NodeType == ExpressionType.Constant)
                {
                    var cont = (ConstantExpression)expression.Arguments[i];
                    Field calcField = new Field("", $"{ConstantValue(cont)}", expression.Members[i].Name, expression.Members[i].Name);
                    campos.Add(calcField);
                    MQB.lstFields.Add(calcField);
                    continue;
                }

                var arg = (MemberExpression)expression.Arguments[i];
                if (arg.Type.IsClass && arg.Type != typeof(string))
                {
                    GetProperties(expression.Members[i].Name, arg.Type, campos);
                    if (arg.Member.Name != expression.Members[i].Name)
                    {
                        var newAdd = MQB.lstFields
                            .Where(t => t.TypeField.Split('.')[0] == arg.Member.Name)
                            .Select(t => new Field(t.Table, t.DbField, expression.Members[i].Name + t.TypeField.Substring(arg.Member.Name.Length), t.DbAlias)).ToList();
                        MQB.lstFields.AddRange(newAdd);
                    }
                }
                else
                {
                    string name = GetMember("", _tipo, arg);
                    campos.Add(new Field("$Member", "", name, expression.Members[i].Name));
                }
            }

            //
            //GetProperties("", expression.Type, campos);
            var result = (from a in MQB.lstFields
                          join b in campos on a.TypeField equals b.TypeField
                          select new Field(a.Table, a.DbField, b.Table == "$Member" ? b.DbAlias : a.TypeField, a.DbAlias) { }).ToList();

            MQB.lstSelect = result;


        }
    }

    public string GetMember(string root, Type _tipo, Expression exp)
    {
        Expression parameter = null;
        return GetMember(root, _tipo, exp, out parameter);
    }
    public string GetMember(string root, Type _tipo, Expression exp, out Expression parameter)
    {
        parameter = exp;
        if (exp.NodeType == ExpressionType.Parameter)
            return root;


        string separador = string.IsNullOrEmpty(root) ? "" : ".";
        if (exp.NodeType == ExpressionType.MemberAccess)
        {
            var member = (MemberExpression)exp;
            parameter = member.Expression;
            if (member.Expression.NodeType == ExpressionType.MemberAccess && member.Expression.Type.IsClass && member.Expression.Type != typeof(string))
                return GetMember($"{Modulo.GetColumnName(member.Expression.Type, member.Member.Name)}{separador}{root}", member.Expression.Type, member.Expression, out parameter);
            else
                return $"{Modulo.GetColumnName(_tipo, member.Member.Name)}{separador}{root}";
        }

        return "";
    }


    public void SelectNewField(Expression exp)
    {

    }

    public void SelectParameter(ParameterExpression expression)
    {
        if (primerSelect)
        {
            primerSelect = false;
            return;
        }
        MQB.tableIndex++;



        //List<Field> _lastCampos = new List<Field>();
        //var properties = expression.Type.GetProperties();
        //for (int a =0; a < properties.Length;a++)
        //{
        //    var property = properties[a];
        //    var lastName = camposSelect.FirstOrDefault(t=> t.DbField == property.Name);
        //    _lastCampos.Add(new Field($"t{MQB.tableIndex}",lastName.DbAlias,lastName.DbAlias));
        //}
        //string newSelect = string.Join(",", _lastCampos.Select(t=> $"{t.Table}.{t.DbField}"));
        //string actualSelect = _selectClause.LastOrDefault();
        //string finalSelect = $"SELECT {newSelect} FROM ({actualSelect}) t{MQB.tableIndex}";
        //_selectClause.Add(finalSelect);
        //camposSelect = _lastCampos;
    }

    protected override Expression VisitUnary(UnaryExpression u)
    {
        switch (u.NodeType)
        {
            case ExpressionType.Not:
                sb.Append(" NOT ");
                this.Visit(u.Operand);
                break;
            case ExpressionType.Convert:
                this.Visit(u.Operand);
                break;
            default:
                throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
        }
        return u;
    }

    protected override Expression VisitBinary(BinaryExpression b)
    {

        sb.Append("(");
        this.Visit(b.Left);

        switch (b.NodeType)
        {
            case ExpressionType.And:
                sb.Append(" AND ");
                break;

            case ExpressionType.AndAlso:
                sb.Append(" AND ");
                break;

            case ExpressionType.Or:
                sb.Append(" OR ");
                break;

            case ExpressionType.OrElse:
                sb.Append(" OR ");
                break;

            case ExpressionType.Equal:
                if (IsNullConstant(b.Right))
                {
                    sb.Append(" IS ");
                }
                else
                {
                    sb.Append(" = ");
                }
                break;

            case ExpressionType.NotEqual:
                if (IsNullConstant(b.Right))
                {
                    sb.Append(" IS NOT ");
                }
                else
                {
                    sb.Append(" <> ");
                }
                break;

            case ExpressionType.LessThan:
                sb.Append(" < ");
                break;

            case ExpressionType.LessThanOrEqual:
                sb.Append(" <= ");
                break;

            case ExpressionType.GreaterThan:
                sb.Append(" > ");
                break;

            case ExpressionType.GreaterThanOrEqual:
                sb.Append(" >= ");
                break;

            default:
                throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));

        }

        if (b.Right.NodeType == ExpressionType.New)
            sb.Append(NewValue((NewExpression)b.Right));
        else
            this.Visit(b.Right);
        sb.Append(")");
        return b;
    }

    protected override Expression VisitConstant(ConstantExpression c)
    {
        IQueryable q = c.Value as IQueryable;

        if (q == null && c.Value == null)
        {
            sb.Append("NULL");
        }
        else if (q == null)
        {
            switch (Type.GetTypeCode(c.Value.GetType()))
            {
                case TypeCode.Boolean:
                    sb.Append(((bool)c.Value) ? 1 : 0);
                    break;

                case TypeCode.String:
                    sb.Append("'");
                    sb.Append(c.Value);
                    sb.Append("'");
                    break;

                case TypeCode.DateTime:
                    sb.Append("'");
                    sb.Append(c.Value);
                    sb.Append("'");
                    break;

                case TypeCode.Object:
                    throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", c.Value));

                default:
                    sb.Append(c.Value);
                    break;
            }
        }

        return c;
    }

    protected override Expression VisitMember(MemberExpression m)
    {
        if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
        {
            string typeField = Modulo.GetColumnName(this.tipo, m.Member.Name);
            sb.Append($"t{_selectIndex}." + typeField);
            return m;
        }

        string[] strExpression = m.ToString().Split('.');

        if (strExpression.Length > 0 && strExpression[0] == parameterWhere)
        {
            strExpression[0] = null;
            string typeField = string.Join(".", strExpression, 1, strExpression.Length - 1);
            var field = MQB.lstFields.FirstOrDefault(t => t.TypeField == typeField);

            sb.Append($"{field.Table}.{field.DbField}");
            return m;
        }

        if (m.Expression == null
        || (m.Expression != null && m.Expression.NodeType == ExpressionType.Constant)
        || m.NodeType == ExpressionType.MemberAccess)
        {
            LambdaExpression lambda = Expression.Lambda(m);

            Delegate fn = lambda.Compile();
            var ConstantExp = Expression.Constant(fn.DynamicInvoke(null), m.Type);

            sb.Append(ConstantValue(ConstantExp));
            return ConstantExp;
        }

        throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
    }

    public object ConstantValue(ConstantExpression expresion)
    {
        if (expresion.Value == null)
            return "NULL";
        else if (expresion.Type == typeof(string) ||
            expresion.Type == typeof(char) ||
            expresion.Type == typeof(TimeSpan))
        {
            return "'" + expresion.Value.ToString() + "'";
        }
        else if (expresion.Type == typeof(DateTime))
            return "'" + ((DateTime)expresion.Value).ToString("yyyy-MM-dd HH:mm:ss") + "'";
        else if (expresion.Type == typeof(bool))
            return Convert.ToInt16(expresion.Value).ToString();

        return expresion.Value;
    }

    public object NewValue(NewExpression expresion)
    {
        var dele = Expression.Lambda(expresion).Compile();
        var result = dele.DynamicInvoke();
        Type resultType = result.GetType();

        if (resultType == typeof(string) ||
            resultType == typeof(char) ||
            resultType == typeof(TimeSpan))
        {
            return "'" + result.ToString() + "'";
        }
        else if (resultType == typeof(DateTime))
            return "'" + ((DateTime)result).ToString("yyyy-MM-dd HH:mm:ss") + "'";

        return result.ToString();

    }


    protected bool IsNullConstant(Expression exp)
    {
        return (exp.NodeType == ExpressionType.Constant && ((ConstantExpression)exp).Value == null);
    }

    private bool ParseOrderByExpression(MethodCallExpression expression, string order)
    {
        UnaryExpression unary = (UnaryExpression)expression.Arguments[1];
        LambdaExpression lambdaExpression = (LambdaExpression)unary.Operand;

        lambdaExpression = (LambdaExpression)Evaluator.PartialEval(lambdaExpression);

        MemberExpression body = lambdaExpression.Body as MemberExpression;
        string name = GetMember("", lambdaExpression.Parameters[0].Type, body);

        if (body != null)
        {
            if (string.IsNullOrEmpty(_orderBy))
            {
                _orderBy = string.Format("{0} {1}", name, order);
                var field = MQB.lstFields.FirstOrDefault(t => t.TypeField == name);
                MQB.lstOrder.Add(new Field(field.Table, field.DbField, field.TypeField, order));
            }
            else
            {
                _orderBy = string.Format("{0}, {1} {2}", _orderBy, name, order);
            }

            return true;
        }

        return false;
    }

    private bool ParseTakeExpression(MethodCallExpression expression)
    {
        ConstantExpression sizeExpression = (ConstantExpression)expression.Arguments[1];

        int size;
        if (int.TryParse(sizeExpression.Value.ToString(), out size))
        {
            this.MQB.Top = size.ToString();
            return true;
        }

        return false;
    }

    private bool ParseSkipExpression(MethodCallExpression expression)
    {
        ConstantExpression sizeExpression = (ConstantExpression)expression.Arguments[1];

        int size;
        if (int.TryParse(sizeExpression.Value.ToString(), out size))
        {
            _skip = size;
            return true;
        }

        return false;
    }

    private Expression FirstExprresion(Expression m)
    {

        if (m is ConstantExpression)
            return m;

        MethodCallExpression exp = (MethodCallExpression)m;
        lsExpressios.Insert(0, exp);
        while (true)
        {
            if (exp.Arguments[0] is MethodCallExpression)
            {
                exp = (MethodCallExpression)exp.Arguments[0];
                lsExpressios.Insert(0, exp);
            }
            else if (exp.Arguments[0] is ConstantExpression)
            {
                return (Expression)exp.Arguments[0];
            }
            else
                break;

        }
        return exp;
    }

    private void GetProperties(string root, Type tipo, List<Field> lst)
    {
        string separador = string.IsNullOrEmpty(root) ? "" : ".";
        foreach (PropertyInfo property in tipo.GetProperties())
        {
            if (property.PropertyType.IsClass && property.PropertyType != typeof(string))

                GetProperties($"{root}{separador}{property.Name}", property.PropertyType, lst);
            else
                lst.Add(new MyORM.Field("", $"{property.GetColumnName()}", $"{root}{separador}{property.GetColumnName()}", property.Name));

        }
    }
}

