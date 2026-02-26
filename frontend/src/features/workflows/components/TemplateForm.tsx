import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import CodeEditor from '../../../shared/components/CodeEditor';

const schema = z.object({
  name: z.string().min(1, 'Required'),
  service: z.string().min(1, 'Required'),
  description: z.string().optional(),
  steps_json: z.string().min(1, 'Required'),
});

type FormValues = z.output<typeof schema>;

interface Props {
  onSubmit: (values: { name: string; service: string; description?: string; steps: Record<string, unknown> }) => void;
  onCancel: () => void;
  loading?: boolean;
}

const services = ['snorkel', 'occupancy', 'hvac', 'general'];

const inputClass = 'w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500';

const STEPS_TEMPLATE = JSON.stringify({
  steps: [
    {
      key: 'step_1',
      dagster_job: 'job_name',
      config_template: { ops: {} },
      depends_on: [],
    },
  ],
}, null, 2);

export default function TemplateForm({ onSubmit, onCancel, loading }: Props) {
  const { register, handleSubmit, formState: { errors }, setValue, watch } = useForm({
    resolver: zodResolver(schema),
    defaultValues: { name: '', service: 'snorkel', description: '', steps_json: STEPS_TEMPLATE },
  });

  const stepsJson = watch('steps_json');

  const handleFormSubmit = (values: FormValues) => {
    try {
      const steps = JSON.parse(values.steps_json);
      onSubmit({ name: values.name, service: values.service, description: values.description, steps });
    } catch {
      // zod validation should catch this, but just in case
    }
  };

  return (
    <form onSubmit={handleSubmit(handleFormSubmit)} className="space-y-4">
      <div className="grid grid-cols-2 gap-4">
        <Field label="Name" error={errors.name?.message as string}>
          <input {...register('name')} className={inputClass} />
        </Field>
        <Field label="Service" error={errors.service?.message as string}>
          <select {...register('service')} className={inputClass}>
            {services.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <Field label="Description" error={errors.description?.message as string}>
        <textarea {...register('description')} className={`${inputClass} min-h-[60px] resize-y`} />
      </Field>
      <div>
        <CodeEditor
          label="Steps (JSON)"
          value={stepsJson}
          onChange={(e) => setValue('steps_json', e.target.value)}
          error={errors.steps_json?.message as string}
        />
      </div>
      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onCancel} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={loading} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
          {loading ? 'Creating...' : 'Create Template'}
        </button>
      </div>
    </form>
  );
}

function Field({ label, error, children }: { label: string; error?: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>
      {children}
      {error && <p className="text-xs text-red-500 mt-1">{error}</p>}
    </div>
  );
}
