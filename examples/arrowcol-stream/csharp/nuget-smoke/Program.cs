using System;
using System.Runtime.InteropServices;
using Arrowcol.Client;

var assembly = typeof(ArrowcolContract).Assembly;
var library = NativeLibrary.Load("arrowcol_sdk_ffi", assembly, null);
try
{
    foreach (var export in new[] {
        "arrowcol_open", "arrowcol_push", "arrowcol_flush",
        "arrowcol_close", "arrowcol_destroy", "arrowcol_free_error"
    })
        NativeLibrary.GetExport(library, export);
    Console.WriteLine($"ArrowCol native API loaded via NuGet on {RuntimeInformation.RuntimeIdentifier}");
}
finally
{
    NativeLibrary.Free(library);
}
