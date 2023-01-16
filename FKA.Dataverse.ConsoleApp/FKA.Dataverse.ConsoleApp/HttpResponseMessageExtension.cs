using PowerApps.Samples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Threading.Tasks;

namespace FKA.Dataverse.ConsoleApp
{
    internal static class HttpResponseMessageExtension
    {
        public static EntityReference? GetEntityReference(this HttpResponseMessage httpResponseMessage)
        {


                if (httpResponseMessage.Headers != null &&
                    httpResponseMessage.Headers.Contains("OData-EntityId") &&
                    httpResponseMessage.Headers.GetValues("OData-EntityId") != null)
                {
                    return new EntityReference(httpResponseMessage.Headers.GetValues("OData-EntityId").FirstOrDefault());
                }
                else
                {

                    return null;
                }
            
        }
    }
}
