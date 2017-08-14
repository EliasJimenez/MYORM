using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace MyORM
{
    public class MyQueryProvider : IQueryProvider
    {
        private DbContext _context;

        public MyQueryProvider(DbContext context)
        {
            _context = context;
        }
       public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(MyQueryable<>).MakeGenericType(elementType), new object[] { this, expression,_context });
            }
            catch (System.Reflection.TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public IQueryable<T> CreateQuery<T>(Expression expression)
        {
            return new MyQueryable<T>(this, expression, _context);
        }

        public object Execute(Expression expression)
        {
            var arguments = expression.Type.GetGenericArguments();
            Type tipoResult = arguments.Length > 0 ? arguments[0] : expression.Type;
            return Execute(tipoResult, expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            return (TResult)Execute(typeof(TResult), expression);
        }

        public Object Execute(Type typeResult, Expression expression)
        {
            Expression primeraExpression = this.FirstExprresion(expression);
            Type collectionType = typeResult;

            var arguments = collectionType.GetGenericArguments();
            Type tipoResult = arguments.Length > 0 ? arguments[0] : collectionType;
            Type tipoBase = primeraExpression.Type.GetGenericArguments()[0];

            bool IsEnumerable = (collectionType.Name == "IEnumerable`1" || collectionType == tipoResult);

            if (expression is MethodCallExpression)
            {
                MethodCallExpression exp = expression as MethodCallExpression;

                if (exp.Method.Name == "FirstOrDefault" || exp.Method.Name == "Count" || exp.Method.Name == "Any"
                     || exp.Method.Name == "Max" || exp.Method.Name == "Min")
                {
                    var query = (IQueryable)Activator.CreateInstance(typeof(MyQueryable<>).MakeGenericType(tipoBase), new object[] { this, expression, _context });

                    var result = (IList)_context.Consulta(collectionType, query.ToString());

                    return First(result);
                }
            }

            if (IsEnumerable)
            {
                var query = (IQueryable)Activator.CreateInstance(typeof(MyQueryable<>).MakeGenericType(tipoBase), new object[] { this, expression, _context });

                var result = _context.Consulta(tipoResult, query.ToString());

                return result;
            }



            return CreateQuery(expression);
        }

        public object First(IList lista)
        {
            return (lista.Count > 0 ? lista[0] : null);
        }

        private Expression FirstExprresion(Expression m)
        {

            if (m is ConstantExpression)
                return m;

            MethodCallExpression exp = (MethodCallExpression)m;
            while (true)
            {
                if (exp.Arguments[0] is MethodCallExpression)
                {
                    exp = (MethodCallExpression)exp.Arguments[0];
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


    }
}
