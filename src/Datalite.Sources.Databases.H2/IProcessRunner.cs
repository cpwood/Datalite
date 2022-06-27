using System.Diagnostics;
using System.Threading.Tasks;

namespace Datalite.Sources.Databases.H2
{
    internal interface IProcessRunner
    {
        Task<string> RunAsync(ProcessStartInfo psi);
    }
}