using SimpleBank.Api;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(new Bank(accountCount: 10000, initialBalance: 1000m)); // paralel Linq için 10.000 ile 1.000.000 arasındaki farkı gösterelim.

var app = builder.Build();

app.MapGet("/api/accounts", (Bank bank) =>
    bank.Accounts.Select(a => new { a.Id, a.Balance }));

app.MapGet("/api/total", (Bank bank) => new { Total = bank.Total() }); // kilit uygulamayıncaki durumu gösterelim.

app.MapPost("/api/reset", (Bank bank) =>
{
    bank.Reset(1000m);
    return Results.Ok();
});

// Tek transfer (lock ile güvenli)
app.MapPost("/api/transfer", (int fromId, int toId, decimal amount, Bank bank) =>
{
    var from = bank.Find(fromId);
    var to = bank.Find(toId);
    if (from is null || to is null) return Results.NotFound("Hesap yok.");

    return Transfers.Safe(from, to, amount)
        ? Results.Ok()
        : Results.BadRequest("Transfer başarısız.");
});

// 1-3: Parallel.For, race condition, lock, Interlocked
app.MapPost("/api/race", (int count, bool safe, Bank bank) =>
    Simulations.Race(bank, count, safe));

// 4: Parallel.ForEach ile toplu faiz
app.MapPost("/api/interest", (decimal rate, Bank bank) =>
    new { Total = Simulations.ApplyInterest(bank, rate) });

// 5: PLINQ raporu
app.MapGet("/api/reportParallelOrdered", (Bank bank) => Simulations.ReportParallelOrdered(bank));

// 6: PLINQ raporu No Ordered
app.MapGet("/api/reportParallelNoOrder", (Bank bank) => Simulations.ReportParallelNoOrder(bank));

// 7: LINQ raporu
app.MapGet("/api/report", (Bank bank) => Simulations.ReportSync(bank));


// 8: SemaphoreSlim + async/await
app.MapPost("/api/transfers/async", (int count, int maxConcurrency, Bank bank, CancellationToken ct) =>
    Simulations.AsyncTransfers(bank, count, maxConcurrency, ct));

// 9: ParallelForAsync
app.MapPost("/api/transfers/parallel-async", (int count, int maxConcurrency, Bank bank, CancellationToken ct) =>
    Simulations.AsyncTransfersParallel(bank, count, maxConcurrency, ct));

app.Run();
