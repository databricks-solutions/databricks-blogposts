/** Starter questions per demo persona. Most depend on something a technician explained on an earlier shift. */
export const PROMPTS: Record<string, string[]> = {
  rosa: [
    "Conveyor CV-312 stopped with a drive fault alarm. Should I order a new motor drive?",
    "What was total downtime on Line 3 in May 2026, in hours?",
    "The motor drive on conveyor CV-305 is dead. What's the fastest way to replace it?",
    "Who do I call about a press safety issue on nights?",
  ],
  jamal: [
    "How do I replace the drive belt on CV-301?",
    "CV-308 faulted with F-0021 again. Can you pull its work order history?",
  ],
  maria: [
    "Press HP-208 just set off the high-temperature alarm at 2am. Should I send someone out?",
    "I need a cylinder seal kit for HP-207. Which part number and from whom?",
    "Who do I call about a press safety issue on nights?",
  ],
};

export const DEFAULT_PROMPTS = [
  "Which hydraulic presses had the most work orders last quarter?",
  "What alarms has HP-208 thrown this month?",
];
