import { useEffect, useState } from "react";
import { Chat } from "@/components/Chat";
import { MemoryPanel } from "@/components/MemoryPanel";
import { Sidebar } from "@/components/Sidebar";
import { TopBar } from "@/components/TopBar";
import { useChat } from "@/stores/chat";

export default function App() {
  const { me, init } = useChat();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    init().catch((e: Error) => setError(e.message));
  }, [init]);

  return (
    <div className="flex h-full flex-col">
      <TopBar me={me} />
      {error && <div className="bg-lava-50 px-4 py-2 text-xs text-lava-600">Couldn't reach MaintBot: {error}</div>}
      <div className="flex min-h-0 flex-1">
        <Sidebar />
        <Chat />
        <MemoryPanel />
      </div>
    </div>
  );
}
