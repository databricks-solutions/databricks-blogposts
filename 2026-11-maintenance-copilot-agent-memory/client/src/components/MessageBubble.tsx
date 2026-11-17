import { ExclamationTriangleIcon as AlertTriangle } from "@heroicons/react/24/outline";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { ChatMessage } from "@/types";
import { ToolChips } from "./ToolChips";

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  if (message.role === "user") {
    return (
      <div className="flex justify-end">
        <div className="max-w-[78%] rounded-2xl rounded-br-sm bg-navy px-3.5 py-2 text-[14px] leading-relaxed text-white">
          {message.text}
        </div>
      </div>
    );
  }
  return (
    <div className="flex gap-3">
      <div className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-lava text-2xs font-bold text-white" aria-hidden>
        M
      </div>
      <div className="min-w-0 flex-1">
        <ToolChips calls={message.toolCalls} />
        {message.text ? (
          <div className="prose prose-sm max-w-none text-navy prose-headings:text-navy prose-strong:text-navy prose-code:font-mono prose-code:text-[12px] prose-code:before:content-none prose-code:after:content-none prose-table:text-xs prose-th:bg-oat-100 prose-th:px-2 prose-td:px-2">
            <ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml>
              {message.text}
            </ReactMarkdown>
          </div>
        ) : (
          message.pending && (
            <div className="space-y-2 pt-1" aria-label="MaintBot is working">
              <div className="skeleton h-3 w-3/4" />
              <div className="skeleton h-3 w-1/2" />
            </div>
          )
        )}
        {message.error && (
          <p className="mt-2 flex items-center gap-1.5 text-xs text-lava-600">
            <AlertTriangle className="h-3.5 w-3.5" aria-hidden /> {message.error}
          </p>
        )}
      </div>
    </div>
  );
}
