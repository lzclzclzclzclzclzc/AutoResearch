import type { Data } from './api';

export type ConversationSelection = { scope: string; id: string } | null;
export const conversationId = (item: Data) => String(item.conversation_id || item.id || '');
const timestamp = (value: unknown) => typeof value === 'string' ? Date.parse(value) || 0 : 0;

/** Keep the same stable ordering for API results, refreshes and history cursors. */
export function sortConversations(items: Data[]): Data[] {
  return [...items].sort((a, b) => Number(b.is_active ?? b.activity_status === 'running') - Number(a.is_active ?? a.activity_status === 'running') ||
    timestamp(b.last_activity_at || b.created_at) - timestamp(a.last_activity_at || a.created_at) ||
    Number(b.last_activity_seq || 0) - Number(a.last_activity_seq || 0) || conversationId(b).localeCompare(conversationId(a)));
}

/** No pin means automatic selection follows current activity on every refresh. */
export function selectedConversation(items: Data[], selection: ConversationSelection, scope: string): Data | undefined {
  return (selection?.scope === scope && items.find(item => conversationId(item) === selection.id)) || items[0];
}
