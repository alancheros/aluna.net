namespace Paradigma.EventSourcing.Abstractions;


public abstract class VersionedContractBase
{
    public int ContractVersion { get; }

    protected VersionedContractBase(int contractVersion)
    {
        if (contractVersion <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(contractVersion), "ContractVersion must be greater than 0.");
        }

        ContractVersion = contractVersion;
    }
}
