using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace ProtobufMapper
{
    public class MapTypeConfiguration
    {
        internal Dictionary<PropertyInfo, PropertyConfiguration> propertyConfigurations = new Dictionary<PropertyInfo, PropertyConfiguration>();
        
    }

    public class MapTypeConfiguration<T> : MapTypeConfiguration
    {
        public MapTypeConfiguration<T> Property<TP>(Expression<Func<T, TP>> property, ulong order)
        {
            if (property == null)
            {
                throw new ArgumentNullException(nameof(property));
            }

            var body = property.Body;
            if (body.NodeType == ExpressionType.MemberAccess)
            {
                var memAccess = (MemberExpression)body;
                var propertyInfo = (PropertyInfo)memAccess.Member;
                propertyConfigurations.Add(propertyInfo, new PropertyConfiguration { Order = order });
            }
            return this;
        }
    }
}