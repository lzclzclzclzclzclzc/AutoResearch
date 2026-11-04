import { api, items } from './api.ts';
import type { Data } from './api';

export function timelineQueryOptions(researchId: string, history: boolean) {
  return {
    queryKey: ['timeline', researchId],
    queryFn: async () => {
      let result: Data[] = [];
      let after = 0;
      for (;;) {
        const page = await api(`/api/research/${researchId}/timeline?after=${after}&limit=2000`);
        const rows = items(page);
        result = result.concat(rows);
        if (rows.length < 2000) break;
        after = rows.at(-1)!.seq;
      }
      return result;
    },
    enabled: history,
    // Re-entering history (including a reused workspace) must check for new events.
    staleTime: 0,
  };
}
