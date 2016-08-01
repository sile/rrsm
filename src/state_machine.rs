pub trait Machine: Default {
    type Command;
    type Snapshot: Into<Self>;
    fn execute(&mut self, command: Self::Command);
    fn take_snapshot(&self) -> Self::Snapshot;
}
