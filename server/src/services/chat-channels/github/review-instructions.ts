import { createHash } from "node:crypto";
import { and, desc, eq, inArray } from "drizzle-orm";
import { chatConversations, chatDeliveries, chatEndpoints, chatMessageLinks, type Db } from "@paperclipai/db";
import { githubReviewConfigurationPrompt } from "../../chat-github-review-policy.js";
import type { GitHubReviewPolicy } from "@paperclipai/shared";

/** Only processed, task-bound automatic deliveries can supply review instructions. */
export async function resolveGitHubReviewInstructions(input: {
  db: Db; companyId: string; agentId: string; issueId: string; commentIds: string[];
}): Promise<Array<{ key: string; instructions: string }>> {
  const rows = await input.db.select({ endpointId: chatEndpoints.id, event: chatDeliveries.normalizedEvent })
    .from(chatMessageLinks)
    .innerJoin(chatDeliveries, and(
      eq(chatDeliveries.companyId, chatMessageLinks.companyId),
      eq(chatDeliveries.endpointId, chatMessageLinks.endpointId),
      eq(chatDeliveries.conversationId, chatMessageLinks.conversationId),
      eq(chatDeliveries.id, chatMessageLinks.deliveryId),
    ))
    .innerJoin(chatConversations, and(
      eq(chatConversations.companyId, chatMessageLinks.companyId),
      eq(chatConversations.endpointId, chatMessageLinks.endpointId),
      eq(chatConversations.id, chatMessageLinks.conversationId),
    ))
    .innerJoin(chatEndpoints, and(eq(chatEndpoints.companyId, chatConversations.companyId), eq(chatEndpoints.id, chatConversations.endpointId)))
    .where(and(
      eq(chatMessageLinks.companyId, input.companyId), eq(chatMessageLinks.direction, "inbound"),
      eq(chatConversations.issueId, input.issueId), eq(chatEndpoints.assignedAgentId, input.agentId),
      eq(chatEndpoints.provider, "github"), eq(chatDeliveries.state, "processed"),
      ...(input.commentIds.length ? [inArray(chatMessageLinks.commentId, input.commentIds)] : []),
    ))
    .orderBy(desc(chatDeliveries.createdAt), desc(chatDeliveries.id))
    .limit(input.commentIds.length || 1);
  const configurations = new Map<string, { key: string; instructions: string }>();
  for (const row of rows) {
    const automatic = row.event.githubAutomatic as {
      policy?: GitHubReviewPolicy; revision?: number; context?: { repositoryId?: string };
    } | undefined;
    if (!automatic?.policy || !Number.isInteger(automatic.revision) || !automatic.context?.repositoryId) continue;
    const instructions = githubReviewConfigurationPrompt(automatic.policy, automatic.revision!);
    const key = createHash("sha256").update(JSON.stringify([row.endpointId, automatic.context.repositoryId, automatic.revision, instructions])).digest("hex");
    configurations.set(key, { key, instructions });
  }
  return [...configurations.values()];
}
