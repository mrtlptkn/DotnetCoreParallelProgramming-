namespace SimpleBank.Api;

public sealed class Account
{
    public Account(int id, decimal balance)
    {
        Id = id;
        Balance = balance;
    }

    public int Id { get; }
    public decimal Balance { get; set; }

    // Bu hesaba erişirken lock (SyncRoot) kullanılır
    public object SyncRoot { get; } = new();
}

public sealed class Bank
{
    public Bank(int accountCount, decimal initialBalance)
    {
        Accounts = Enumerable.Range(1, accountCount)
            .Select(i => new Account(i, initialBalance))
            .ToArray();
    }

    public Account[] Accounts { get; }

    public Account? Find(int id) => Accounts.FirstOrDefault(a => a.Id == id);

    public void Reset(decimal balance)
    {
        foreach (var a in Accounts)
            lock (a.SyncRoot) a.Balance = balance;
    }

    public decimal Total() => Accounts.Sum(a => { return a.Balance; });
}

public static class Transfers
{
    // Kilitsiz: race condition'ı göstermek için (ders amaçlı)
    public static bool Unsafe(Account from, Account to, decimal amount)
    {
        var fromBalance = from.Balance;
        if (amount <= 0 || fromBalance < amount) return false;

        Thread.Yield(); // race penceresini büyütmek için ders amaçlı gecikme

        from.Balance = fromBalance - amount; // eski değere göre yazıyor
        to.Balance += amount;
        return true;
    }

    // lock ile güvenli. Kilitler her zaman Id sırasıyla alınır (deadlock önleme).
    public static bool Safe(Account from, Account to, decimal amount)
    {
        if (amount <= 0 || from.Id == to.Id) return false;

        var (first, second) = from.Id < to.Id ? (from, to) : (to, from);

        lock (first.SyncRoot) 
        lock (second.SyncRoot)
        {
            if (from.Balance < amount) return false;

            from.Balance -= amount;
            to.Balance += amount;
            return true;
        }
    }
}
