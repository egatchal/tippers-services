import { forwardRef } from 'react';

interface Props extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  label?: string;
  error?: string;
}

const SqlEditor = forwardRef<HTMLTextAreaElement, Props>(({ label, error, className = '', ...props }, ref) => (
  <div>
    {label && <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>}
    <textarea
      ref={ref}
      className={`w-full min-h-[120px] p-3 font-mono text-sm bg-gray-900 text-green-400 rounded-md border ${
        error ? 'border-red-400' : 'border-gray-700'
      } focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y ${className}`}
      spellCheck={false}
      {...props}
    />
    {error && <p className="text-xs text-red-500 mt-1">{error}</p>}
  </div>
));

SqlEditor.displayName = 'SqlEditor';
export default SqlEditor;
