using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace MyORM
{
    public class MyQueryable<T> : IQueryable<T> , IOrderedQueryable<T>
    {
        private readonly IQueryProvider _provider;
        private Expression _expression;
        private DbContext _context;

        public MyQueryable(MyQueryProvider provider,DbContext context)
        {
            _context = context;
            _provider = provider;
            _expression = Expression.Constant(this);
        }
        public MyQueryable(MyQueryProvider provider, Expression _expression, DbContext context)
        {
            _context = context;
            _provider = provider;
            this._expression = _expression;
        }

        public DbContext Context
        {
            get
            {
                return this._context;
            }
        }
        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)Provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Provider.Execute(Expression)).GetEnumerator();
        }

        public Expression Expression
        {
            get
            {
                return _expression;
            }
        }

        public Type ElementType
        {
            get
            {
                return typeof(T);
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                return _provider;
            }
        }

        public override string ToString()
        {
            
            var qt = new QueryTranslator(typeof(T),Context.GetDBQuery());
            string filtro = qt.Translate(this.Expression).Trim();
            Context.Mapping = qt.Mapping;
           // filtro = filtro.Length > 0 ? "Where "+ filtro  : filtro;
            var result = $"{filtro}";
            return filtro;
        }
    }
}
