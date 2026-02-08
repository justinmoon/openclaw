/**
 * High-level convenience function for external channel plugins to route an
 * inbound message through the OpenClaw agent and deliver the reply.
 *
 * Built-in channels wire up debouncers, DM policies, mention gating, etc.
 * themselves.  This helper provides a minimal "just call the agent and reply"
 * path so that external plugins (loaded via jiti from ~/.openclaw/extensions)
 * can invoke the agent without duplicating the full built-in channel plumbing.
 */

import type { MsgContext } from "../templating.js";
import type { ReplyPayload } from "../types.js";
import { loadConfig } from "../../config/config.js";
import { getReplyFromConfig } from "./get-reply.js";
import { finalizeInboundContext } from "./inbound-context.js";

export type HandleInboundMessageParams = {
  /** Channel identifier (e.g. "marmot", "nostr"). */
  channel: string;
  /** Account id within the channel (usually "default"). */
  accountId: string;
  /** Sender identifier (pubkey, phone number, etc.). */
  senderId: string;
  /** "direct" or "group". */
  chatType: "direct" | "group";
  /** Chat / conversation identifier. */
  chatId: string;
  /** The message text. */
  text: string;
  /** Optional sender display name. */
  senderName?: string;
  /** Callback to deliver the agent's text reply. */
  reply: (text: string) => Promise<void>;
};

export async function handleInboundMessage(params: HandleInboundMessageParams): Promise<void> {
  const { channel, accountId, senderId, chatType, chatId, text, senderName, reply } = params;

  // Build session key: channel:accountId:chatId (mirrors the pattern used by built-in channels).
  const sessionKey = `${channel}:${accountId}:${chatId}`;

  // Construct the MsgContext that the agent system expects.
  const ctx: MsgContext = {
    Body: text,
    RawBody: text,
    CommandBody: text,
    BodyForCommands: text,
    From: senderId,
    To: chatId,
    SessionKey: sessionKey,
    AccountId: accountId,
    Provider: channel,
    Surface: channel,
    ChatType: chatType,
    SenderId: senderId,
    SenderName: senderName,
    // Allow text commands (e.g. /reset) from all senders in this simple path.
    CommandAuthorized: true,
  };

  // Finalize the context (normalizes newlines, sets BodyForAgent, etc.).
  const finalized = finalizeInboundContext(ctx, {
    forceBodyForAgent: true,
    forceBodyForCommands: true,
    forceChatType: true,
    forceConversationLabel: true,
  });

  const cfg = loadConfig();

  // Call the agent.
  const result = await getReplyFromConfig(finalized, undefined, cfg);

  // Extract text from the reply payload(s) and deliver via the caller's reply callback.
  if (!result) {
    return;
  }

  const payloads: ReplyPayload[] = Array.isArray(result) ? result : [result];
  for (const payload of payloads) {
    const replyText = payload.text?.trim();
    if (replyText) {
      await reply(replyText);
    }
  }
}
