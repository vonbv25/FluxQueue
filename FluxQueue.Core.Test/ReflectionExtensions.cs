using System.Reflection;

namespace FluxQueue.Tests;

internal static class ReflectionExtensions
{
    internal static T GetPrivateField<T>(this object obj, string fieldName)
    {
        var f = obj.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
        if (f is null) throw new InvalidOperationException($"Field not found: {fieldName}");
        return (T)f.GetValue(obj)!;
    }
}