using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Datalite
{
    /// <summary>
    /// Base class from which other data source configurations will inherit.
    /// </summary>
    public abstract class DataliteContext
    {
        private readonly Func<DataliteContext, Task> _executor;

        /// <summary>
        /// The indexes that will be applied to the table in Sqlite.
        /// </summary>
        public List<string[]> Indexes { get; }

        protected DataliteContext(Func<DataliteContext, Task> executionFunction)
        {
            _executor = executionFunction;
            Indexes = new List<string[]>();
        }
        
        /// <summary>
        /// Run the code that has been provided for execution when the user is ready to start the work.
        /// </summary>
        /// <returns></returns>
        public Task ExecuteAsync()
        {
            return _executor(this);
        }
    }
}
