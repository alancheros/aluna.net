Building the project in Release configuration and packing it into a NuGet package.

```
dotnet clean ".\src\Aluna.net\Aluna.net\Aluna.net.csproj" -c Release
dotnet restore ".\src\Aluna.net\Aluna.net\Aluna.net.csproj"
dotnet pack ".\src\Aluna.net\Aluna.net\Aluna.net.csproj" -c Release -o ".\artifacts\nuget" -p:Version=1.1.1
```