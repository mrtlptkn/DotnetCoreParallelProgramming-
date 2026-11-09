using System.Collections.Concurrent;
using System.Diagnostics;

namespace SimpleBank.Api;

public sealed record RaceResult(
    string Mode,
    decimal Expected,
    decimal Actual,
    decimal Difference,
    long SafeSuccessfulTransfers,
    long UnsafeSuccessfulTransfers,
    long ElapsedMs);

public sealed record AsyncResult(
    int MaxConcurrency,
    int Requested,
    long SuccessfulTransfers,
    long ElapsedMs);

public static class Simulations
{
  // 1-3: Parallel.For + lock (veya kilitsiz) + Interlocked sayaç
  public static RaceResult Race(Bank bank, int count, bool safe)
  {
    bank.Reset(1000m);
    var accounts = bank.Accounts;
    var expected = accounts.Length * 1000m; // toplam banka kasasındaki para.
    long unsafesuccessful = 0;
    long safesuccessful = 0;

    var sw = Stopwatch.StartNew();

    Parallel.For(0, count, _ => // 10.000 account sayısı dönüp topluca accountlar arası para transferi yapacağımızı düşünelim.
    {
      // Normal senkron programlamada Random değerleri üretmek için Random.Next kullanılır. Ama bu kodda ParallelFor içerisinde olduğumuzdan Random.Next doğru çalışmaz bu sebeple Thread-Safe versiyonunu yani Random.Shared.Next()
      var from = accounts[Random.Shared.Next(accounts.Length)]; // 25
      var to = accounts[Random.Shared.Next(accounts.Length)]; // 65
      var amount = Random.Shared.Next(1, 100); // random transfer edilecek miktar.
      // her bir random account için random amaount miktarı kadar para transferi yaptık.

      // Thread-Safe çalışması için safe parametresi true gönderilir bu durumda Transfers.Safe methodu yani concurency göre yazılmış kilit uygulanan kod çalışır ve race condition durumu oluşmaz.

      var ok = safe
              ? Transfers.Safe(from, to, amount)
              : Transfers.Unsafe(from, to, amount); // race condition oluşmasını bekliyoruz.

      if (ok)
      {
        Interlocked.Increment(ref safesuccessful); // lock yerine atomik sayaç -> Kaç başarlı transfer oldu hesabı
        unsafesuccessful++; // kilitsiz artırma yaptık
      }
    });

    sw.Stop();

    var actual = bank.Total();
    return new RaceResult(
        Mode: safe ? "lock (Monitor)" : "kilitsiz",
        Expected: expected,
        Actual: actual,
        Difference: actual - expected,
        SafeSuccessfulTransfers: Interlocked.Read(ref safesuccessful),
        UnsafeSuccessfulTransfers: unsafesuccessful,
        ElapsedMs: sw.ElapsedMilliseconds);
  }

  // 4: Parallel.ForEach ile toplu faiz
  public static decimal ApplyInterest(Bank bank, decimal rate)
  {

    Parallel.ForEach(bank.Accounts, acc =>
    {
      Monitor.Enter(acc.SyncRoot);
      try
      {
        acc.Balance += Math.Round(acc.Balance * rate, 2);
      }
      finally
      {
        Monitor.Exit(acc.SyncRoot);
      }
      ;
    });

    return bank.Total();
  }

  // 5: PLINQ. GroupBy ve Sum, sonra AsOrdered ile sıralı filtreleme
  public static object ReportParallelOrdered(Bank bank)
  {


    var groups = bank.Accounts.AsParallel()
            .GroupBy(a => a.Balance >= 1000m ? "Yüksek" : "Düşük")
            .Select(g => new { Category = g.Key, Count = g.Count(), Total = g.Sum(a => a.Balance) })
            .OrderBy(g => g.Category)
            .ToList();

    var rich = bank.Accounts.AsParallel()
        .AsOrdered() // Paralel işlemi sıralı işlemeye zorladık. 
        .WithDegreeOfParallelism(Environment.ProcessorCount)
        .Where(a => a.Balance >= 1000m)
        .Select(a => new { a.Id, a.Balance })
        .ToList();


    return new { groups, rich };
  }


  public static object ReportParallelNoOrder(Bank bank)
  {

    //ConcurrentBag<Account> accs = new();
    //object syncObject = new();

    ////Paralel ile milyonlarca yada milyarlaca veritabanında çekilen nesneyi işleme alma için AsParallel() sonrası forAll() ile herbir item yakalanır.
    //// Veri sırasını AsParallel ve ForAll garanti etmez ama işlem garantisi ver.
    //// yani sıra garanti etmez derken->veri tabanından çekilen sıra ile threadlerin veriyi işlemesini garanti etmez ama işlem sonucu granti eder.
    //bank.Accounts.AsParallel().ForAll(item =>
    //{
    //  lock (syncObject)
    //  {
    //    item.Balance -= 10;
    //    Console.WriteLine($"[Thread Id] -> {Thread.CurrentThread.ManagedThreadId}");
    //    accs.Add(item); // artık paralel işlenip concurent bir liste içine eklendi.
    //  }

    //});


    // ParallelQuery
    var groups = bank.Accounts.AsParallel()
            .GroupBy(a => a.Balance >= 1000m ? "Yüksek" : "Düşük")
            .Select(g => new { Category = g.Key, Count = g.Count(), Total = g.Sum(a => a.Balance) })
            .ToList();

    var rich = bank.Accounts.AsParallel()
        .WithDegreeOfParallelism(Environment.ProcessorCount)
        .Where(a => a.Balance >= 1000m)
        .Select(a => new { a.Id, a.Balance })
        .ToList();


    return new { groups, rich };
  }


  public static object ReportSync(Bank bank)
  {

    Thread.SpinWait(100000);

    var groups = bank.Accounts
        .GroupBy(a => a.Balance >= 1000m ? "Yüksek" : "Düşük")
        .Select(g => new { Category = g.Key, Count = g.Count(), Total = g.Sum(a => a.Balance) })
        .OrderBy(g => g.Category)
        .ToList();

    var rich = bank.Accounts
        .Where(a => a.Balance >= 1000m)
        .Select(a => new { a.Id, a.Balance })
        .ToList();


    return new { groups, rich };
  }

  // 6: SemaphoreSlim ile en fazla maxConcurrency eşzamanlı transfer, async/await
  public static async Task<AsyncResult> AsyncTransfers(
        Bank bank, int count, int maxConcurrency, CancellationToken ct)
  {
    // SemaphoreSlim’in görevi, aynı anda kaç işin bu kritik ya da kaynak kullanan bölgeye girebileceğini sayıyla  sınırlamak. 
    // Gerçek bir veritabanında bağlantı havuzu 4 bağlantıyla sınırlıysa, 100 istek aynı anda gelse bile yalnızca 4'ü bağlantı alır. Diğerleri sırada bekler ve havuz taşmaz.
    // Amaç aşırı yüklenmeyi önleme, Çok sayıda Task aynı anda dış servise istek atarsa servis çökebilir. Sınır bunu engeller.
    // Thread’leri boşa harcamama: Bekleme await ile yapıldığı için thread havuzu tıkanmaz.
    using var gate = new SemaphoreSlim(maxConcurrency, maxConcurrency);
    var accounts = bank.Accounts;
    long successful = 0;

    var sw = Stopwatch.StartNew();


    var tasks = Enumerable.Range(0, count).Select(async _ =>
    {
      await gate.WaitAsync(ct); // kilit masa dolu ise beklet
      try
      {
        var from = accounts[Random.Shared.Next(accounts.Length)];
        var to = accounts[Random.Shared.Next(accounts.Length)];
        var amount = Random.Shared.Next(1, 100);

        await Task.Delay(50, ct); // simüle edilmiş DB/ağ gecikmesi, kilit dışında

        if (Transfers.Safe(from, to, amount))
          Interlocked.Increment(ref successful);
      }
      finally
      {
        gate.Release(); // müşteri masadan ayrdıldı. yenisini alabiliriz.
      }
    });

    await Task.WhenAll(tasks);
    sw.Stop();

    return new AsyncResult(maxConcurrency, count, Interlocked.Read(ref successful), sw.ElapsedMilliseconds);
  }


  // Parallel.ForEachAsync kendi içinde eşzamanlılık sınırı uyguladığı için SemaphoreSlim gerekmez.Sınırı MaxDegreeOfParallelism belirler.
  public static async Task<AsyncResult> AsyncTransfersParallel(
    Bank bank, int count, int maxConcurrency, CancellationToken ct)
  {
    var accounts = bank.Accounts;
    long successful = 0;

    var options = new ParallelOptions
    {
      MaxDegreeOfParallelism = maxConcurrency, // en fazla maxConcurrency eşzamanlı iş
      CancellationToken = ct
    };

    var sw = Stopwatch.StartNew();

    await Parallel.ForEachAsync(Enumerable.Range(0, count), options, async (_, token) =>
    {
      var from = accounts[Random.Shared.Next(accounts.Length)];
      var to = accounts[Random.Shared.Next(accounts.Length)];
      var amount = Random.Shared.Next(1, 100);

      await Task.Delay(50, token); // simüle edilmiş DB/ağ gecikmesi, kilit dışında

      if (Transfers.Safe(from, to, amount))
        Interlocked.Increment(ref successful);
    });

    sw.Stop();

    return new AsyncResult(maxConcurrency, count, Interlocked.Read(ref successful), sw.ElapsedMilliseconds);
  }
}
