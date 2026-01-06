export interface TimelineEvent {
  id: string;
  year: string;
  date: string;
  title: string;
  description: string;
  category: 'mission' | 'discovery' | 'technology';
  imageUrl?: string;
}

export interface TimelineProps {
  events: TimelineEvent[];
}