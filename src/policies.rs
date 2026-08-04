enum Policy {
    IsAdmin,
    CanWriteToServer,
    CanReadFromServer,
}

trait Policy {
    fn apply(...) -> anyhow::Result<()>
}

struct IsAdmin {}
struct CanBanInServer {}
struct CanWriteInServer {}
